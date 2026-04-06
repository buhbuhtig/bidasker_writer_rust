use chrono::Utc;
use futures_util::future::OptionFuture;
use futures_util::{SinkExt, StreamExt};
use memmap2::MmapOptions;
use reqwest::Client;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration, Instant};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tracing::{error, info, warn};
use sysinfo::{System, ProcessesToUpdate};

// ==========================================
// 1. НАСТРОЙКИ И КОНСТАНТЫ
// ==========================================
const LOG_DIR: &str = "/bidasker_writer_rust/log";
const MMAP_DIR: &str = "/bidasker_writer_rust/mmap";

const MAX_COINS: usize = 2000;
const RECORD_SIZE: usize = 128; // 128 кратно размеру кэш-линии (False Sharing fix)
const SHM_SIZE: usize = MAX_COINS * RECORD_SIZE;

const STATS_REPORT_INTERVAL_SEC: u64 = 30;
const LATENCY_CRITICAL_MS: f64 = 500.0;
const WS_TIMEOUT_SEC: u64 = 200; // 3 минуты Ping от Binance + 20 сек

const NUM_WORKERS_SPOT: usize = 3;
const NUM_WORKERS_FUTURES: usize = 8;
const REQUESTED_SIZE_BUFFER: u32 = 2 * 1024 * 1024; // 2 MB

// --- НАСТРОЙКИ ПЛАНОГО ПЕРЕПОДКЛЮЧЕНИЯ ---
const SCHEDULED_RECONNECT_INTERVAL_SEC: u64 = 3600; // 1 час (3600 секунд)
const STREAMS_RESUBSCRIBE_DELAY_SEC: u64 = 5; // Пауза между рестартом воркеров внутри цикла

#[derive(Clone, Copy)]
struct MmapPtr(*mut u8);
unsafe impl Send for MmapPtr {}
unsafe impl Sync for MmapPtr {}

type WsStream = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>;

// ==========================================
// 2. ОПИСАНИЕ СТРУКТУР BINANCE
// ==========================================
#[derive(Deserialize, Debug)]
struct ExchangeInfo {
    symbols: Vec<SymbolInfo>,
}

#[derive(Deserialize, Debug)]
struct SymbolInfo {
    symbol: String,
    status: String,
}

#[derive(Deserialize, Debug)]
struct BookTickerFutures<'a> {
    #[serde(borrow)] s: &'a str,
    #[serde(borrow)] b: &'a str,
    #[serde(borrow, rename = "B")] bq: &'a str,
    #[serde(borrow)] a: &'a str,
    #[serde(borrow, rename = "A")] aq: &'a str,
    #[serde(rename = "E")] e: u64,
}

#[derive(Deserialize, Debug)]
struct BookTickerSpot<'a> {
    #[serde(borrow)] s: &'a str,
    #[serde(borrow)] b: &'a str,
    #[serde(borrow, rename = "B")] bq: &'a str,
    #[serde(borrow)] a: &'a str,
    #[serde(borrow, rename = "A")] aq: &'a str,
}

#[derive(Deserialize, Debug)]
struct BinanceMsgFutures<'a> {
    #[serde(borrow)] data: BookTickerFutures<'a>,
}

#[derive(Deserialize, Debug)]
struct BinanceMsgSpot<'a> {
    #[serde(borrow)] data: BookTickerSpot<'a>,
}

#[derive(Serialize, Debug)]
struct WsCommand {
    method: String,
    params: Vec<String>,
    id: u64,
}

#[derive(Debug)]
enum WorkerCmd {
    Subscribe(Vec<(String, usize)>),
    Unsubscribe(Vec<String>),
    PlannedReconnect, // Команда на бесшовное переподключение
}

// ==========================================
// 3. ФУНКЦИЯ ЗАПИСИ В ПАМЯТЬ
// ==========================================
#[inline(always)]
unsafe fn write_to_mmap(
    ptr: *mut u8, offset: usize, seq: &mut u64,
    b: &str, bq: &str, a: &str, aq: &str, ts: u64,
) {
    let base = ptr.add(offset);
    let seq_ptr = base as *mut AtomicU64;

    *seq += 1;
    (*seq_ptr).store(*seq, Ordering::Relaxed);
    std::sync::atomic::fence(Ordering::Release);

    let write_20 = |offset_add: usize, val: &str| {
        let dest = base.add(offset_add);
        let v_b = val.as_bytes();
        let len = v_b.len().min(20);
        std::ptr::copy_nonoverlapping(v_b.as_ptr(), dest, len);
        if len < 20 {
            std::ptr::write_bytes(dest.add(len), 0, 20 - len);
        }
    };

    write_20(24, b);
    write_20(44, bq);
    write_20(64, a);
    write_20(84, aq);
    std::ptr::write_unaligned(base.add(104) as *mut u64, ts);

    std::sync::atomic::fence(Ordering::Release);
    *seq += 1;
    (*seq_ptr).store(*seq, Ordering::Relaxed);
}

// ==========================================
// 4. СЕТЕВОЙ УРОВЕНЬ И ПРОГРЕВ СОКЕТА
// ==========================================
async fn connect_to_binance_ws(ws_url: &str) -> Result<WsStream, String> {
    let request = ws_url.into_client_request().map_err(|e| format!("Ошибка URL: {}", e))?;
    let host = request.uri().host().ok_or("Нет host")?;
    let port = request.uri().port_u16().unwrap_or(443);
    let addr_str = format!("{}:{}", host, port);

    let addr = tokio::net::lookup_host(&addr_str).await
        .map_err(|e| format!("DNS: {}", e))?.next().ok_or("Нет IP")?;

    let socket = if addr.is_ipv4() { tokio::net::TcpSocket::new_v4() } else { tokio::net::TcpSocket::new_v6() }
        .map_err(|e| format!("Socket: {}", e))?;

    let _ = socket.set_recv_buffer_size(REQUESTED_SIZE_BUFFER);
    let tcp_stream = socket.connect(addr).await.map_err(|e| format!("TCP Connect: {}", e))?;
    let _ = tcp_stream.set_nodelay(true);

    let (ws_stream, _) = tokio_tungstenite::client_async_tls(request, tcp_stream)
        .await.map_err(|e| format!("TLS/WS: {}", e))?;

    Ok(ws_stream)
}

async fn warmup_new_stream(ws_url: String, streams: Vec<String>) -> Result<WsStream, String> {
    let mut ws_stream = connect_to_binance_ws(&ws_url).await?;
    
    if !streams.is_empty() {
        for chunk in streams.chunks(50) {
            let payload = WsCommand {
                method: "SUBSCRIBE".to_string(),
                params: chunk.to_vec(),
                id: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64,
            };
            let msg = tokio_tungstenite::tungstenite::Message::Text(simd_json::to_string(&payload).unwrap().into());
            
            if let Err(e) = ws_stream.send(msg).await {
                return Err(format!("Ошибка отправки SUBSCRIBE: {}", e));
            }
            
            let delay = tokio::time::sleep(Duration::from_millis(300));
            tokio::pin!(delay);
            
            loop {
                tokio::select! {
                    _ = &mut delay => break,
                    msg_opt = ws_stream.next() => {
                        match msg_opt {
                            Some(Ok(_)) => { /* Котировка вычитана и уничтожена. Буфер ОС пуст! */ }
                            Some(Err(e)) => return Err(format!("Ошибка при чтении во время прогрева: {}", e)),
                            None => return Err("Стрим закрыт сервером во время прогрева".to_string()),
                        }
                    }
                }
            }
        }
    }
    
    match tokio::time::timeout(Duration::from_secs(3), ws_stream.next()).await {
        Ok(Some(Ok(_))) => Ok(ws_stream),
        Ok(Some(Err(e))) => Err(format!("Ошибка WS перед передачей: {}", e)),
        Ok(None) => Err("Стрим закрылся перед передачей".to_string()),
        Err(_) => Err("Таймаут: биржа не прислала данные".to_string()),
    }
}

// ==========================================
// 5. ВОРКЕРЫ
// ==========================================
async fn dedicated_ws_worker(
    market: &'static str,
    symbol: String,
    ws_url: String,
    mmap_ptr: MmapPtr,
    offset_idx: usize,
    mut cmd_rx: mpsc::Receiver<WorkerCmd>,
) {
    let stream_name = format!("{}@bookTicker", symbol.to_lowercase());
    let offset = offset_idx * RECORD_SIZE;
    let mut seq = 0u64;
    let tag = format!("{}-VIP {}", market, symbol);

    loop {
        let ws_stream = match connect_to_binance_ws(&ws_url).await {
            Ok(s) => s,
            Err(e) => { error!("[{:<22}] Ошибка: {}", tag, e); sleep(Duration::from_secs(3)).await; continue; }
        };
        
        let (mut write, mut read) = ws_stream.split();
        let payload = WsCommand {
            method: "SUBSCRIBE".to_string(),
            params: vec![stream_name.clone()],
            id: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64,
        };
        let _ = write.send(tokio_tungstenite::tungstenite::Message::Text(simd_json::to_string(&payload).unwrap().into())).await;

        let mut msg_count = 0u64;
        let mut sum_latency = 0.0;
        let mut max_latency = 0.0;
        let mut last_report = Instant::now();
        let mut uptime_start = Instant::now();
        let mut parse_buf = Vec::with_capacity(1024);

        let mut warmup_task: Option<tokio::task::JoinHandle<Result<WsStream, String>>> = None;

        loop {
            tokio::select! {
                cmd = cmd_rx.recv() => {
                    if let Some(WorkerCmd::PlannedReconnect) = cmd {
                        let url = ws_url.clone();
                        let subs = vec![stream_name.clone()];
                        warmup_task = Some(tokio::spawn(async move { warmup_new_stream(url, subs).await }));
                        info!("[{:<22}] Запущен процесс бесшовного переподключения...", tag);
                    }
                }

                // OptionFuture безопасно конвертирует Option<&mut JoinHandle> в Future
                // Это позволяет избежать паники "unwrap() on a None value" без async блока
                res = OptionFuture::from(warmup_task.as_mut()), if warmup_task.is_some() => {
                    warmup_task = None;
                    if let Some(res_inner) = res {
                        match res_inner {
                            Ok(Ok(new_stream)) => {
                                info!("[{:<22}] Закрытие старого соединения (Graceful Close)...", tag);
                                let _ = tokio::time::timeout(Duration::from_secs(2), async {
                                    let _ = write.send(tokio_tungstenite::tungstenite::Message::Close(None)).await;
                                    let _ = write.flush().await;
                                }).await;

                                let (new_write, new_read) = new_stream.split();
                                write = new_write;
                                read = new_read;
                                uptime_start = Instant::now();
                                info!("[{:<22}] Переключение завершено! Старый сокет закрыт, счетчик аптайма сброшен.", tag);
                            }
                            Ok(Err(e)) => error!("[{:<22}] Ошибка Warmup: {}", tag, e),
                            Err(e) => error!("[{:<22}] Warmup panic: {}", tag, e),
                        }
                    }
                }

                ws_msg = tokio::time::timeout(Duration::from_secs(WS_TIMEOUT_SEC), read.next()) => {
                    match ws_msg {
                        Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) => {
                            parse_buf.clear(); parse_buf.extend_from_slice(text.as_bytes());
                            let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

                            if market == "FUTURES" {
                                if let Ok(p) = simd_json::from_slice::<BinanceMsgFutures>(&mut parse_buf) {
                                    let lat = (ts as f64) - (p.data.e as f64);
                                    msg_count += 1; sum_latency += lat; if lat > max_latency { max_latency = lat; }
                                    unsafe { write_to_mmap(mmap_ptr.0, offset, &mut seq, p.data.b, p.data.bq, p.data.a, p.data.aq, p.data.e); }
                                }
                            } else {
                                if let Ok(p) = simd_json::from_slice::<BinanceMsgSpot>(&mut parse_buf) {
                                    msg_count += 1;
                                    unsafe { write_to_mmap(mmap_ptr.0, offset, &mut seq, p.data.b, p.data.bq, p.data.a, p.data.aq, ts); }
                                }
                            }
                        }
                        Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Ping(_)))) => { let _ = write.flush().await; }
                        Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Close(_)))) | Ok(None) | Ok(Some(Err(_))) | Err(_) => {
                            warn!("[{:<22}] Внезапный разрыв. Принудительный реконнект...", tag);
                            break;
                        }
                        _ => {}
                    }
                }
            }

            if last_report.elapsed().as_secs() >= STATS_REPORT_INTERVAL_SEC {
                if msg_count > 0 {
                    let mps = msg_count as f64 / last_report.elapsed().as_secs_f64();
                    let avg_lat = sum_latency / msg_count as f64;
                    let up_m = uptime_start.elapsed().as_secs() / 60;

                    if market == "FUTURES" && max_latency > LATENCY_CRITICAL_MS {
                        warn!(
                            "[{:<22}] Up: {:>3}m | MPS: {:>7.1} | LatAvg: {:>5.1}ms | Max: {:>6.1}ms", 
                            tag, up_m, mps, avg_lat, max_latency
                        );
                    } else {
                        info!(
                            "[{:<22}] Up: {:>3}m | MPS: {:>7.1} | LatAvg: {:>5.1}ms | Max: {:>6.1}ms", 
                            tag, up_m, mps, avg_lat, max_latency
                        );
                    }
                } else {
                    info!("[{:<22}] Тишина (0 сообщений за {}с)", tag, STATS_REPORT_INTERVAL_SEC);
                }
                msg_count = 0; max_latency = 0.0; sum_latency = 0.0; last_report = Instant::now();
            }
        }
    }
}

async fn ws_worker(
    market: &str,
    worker_id: usize,
    ws_url: String,
    mmap_ptr: MmapPtr,
    mut cmd_rx: mpsc::Receiver<WorkerCmd>,
) {
    let mut local_registry: FxHashMap<String, (usize, u64)> = FxHashMap::default();
    let tag = format!("{}-W{}", market, worker_id);

    loop {
        let ws_stream = match connect_to_binance_ws(&ws_url).await {
            Ok(s) => s,
            Err(e) => { error!("[{:<22}] Ошибка: {}", tag, e); sleep(Duration::from_secs(3)).await; continue; }
        };
        
        let (mut write, mut read) = ws_stream.split();
        if !local_registry.is_empty() {
            let streams: Vec<String> = local_registry.keys().map(|s| format!("{}@bookTicker", s.to_lowercase())).collect();
            for chunk in streams.chunks(50) {
                let p = WsCommand { method: "SUBSCRIBE".to_string(), params: chunk.to_vec(), id: 1 };
                let _ = write.send(tokio_tungstenite::tungstenite::Message::Text(simd_json::to_string(&p).unwrap().into())).await;
                sleep(Duration::from_millis(300)).await;
            }
        }

        let mut msg_count = 0u64;
        let mut sum_latency = 0.0;
        let mut max_latency = 0.0;
        let mut last_report = Instant::now();
        let mut uptime_start = Instant::now();
        let mut parse_buf = Vec::with_capacity(2048);

        let mut warmup_task: Option<tokio::task::JoinHandle<Result<WsStream, String>>> = None;

        loop {
            tokio::select! {
                cmd = cmd_rx.recv() => {
                    if let Some(cmd) = cmd {
                        match cmd {
                            WorkerCmd::Subscribe(syms) => {
                                let mut streams = Vec::new();
                                for (sym, offset) in syms {
                                    local_registry.insert(sym.clone(), (offset, 0));
                                    streams.push(format!("{}@bookTicker", sym.to_lowercase()));
                                }
                                for chunk in streams.chunks(50) {
                                    let p = WsCommand { method: "SUBSCRIBE".to_string(), params: chunk.to_vec(), id: 1 };
                                    let _ = write.send(tokio_tungstenite::tungstenite::Message::Text(simd_json::to_string(&p).unwrap().into())).await;
                                    sleep(Duration::from_millis(300)).await;
                                }
                            }
                            WorkerCmd::Unsubscribe(syms) => {
                                let mut streams = Vec::new();
                                for sym in syms {
                                    local_registry.remove(&sym);
                                    streams.push(format!("{}@bookTicker", sym.to_lowercase()));
                                }
                                for chunk in streams.chunks(50) {
                                    let p = WsCommand { method: "UNSUBSCRIBE".to_string(), params: chunk.to_vec(), id: 1 };
                                    let _ = write.send(tokio_tungstenite::tungstenite::Message::Text(simd_json::to_string(&p).unwrap().into())).await;
                                }
                            }
                            WorkerCmd::PlannedReconnect => {
                                let url = ws_url.clone();
                                let subs = local_registry.keys().map(|s| format!("{}@bookTicker", s.to_lowercase())).collect();
                                warmup_task = Some(tokio::spawn(async move { warmup_new_stream(url, subs).await }));
                                info!("[{:<22}] Запущен процесс бесшовного переподключения...", tag);
                            }
                        }
                    }
                }

                // Используем OptionFuture для безопасного и правильного опроса Option<&mut JoinHandle>
                res = OptionFuture::from(warmup_task.as_mut()), if warmup_task.is_some() => {
                    warmup_task = None;
                    if let Some(res_inner) = res {
                        match res_inner {
                            Ok(Ok(new_stream)) => {
                                info!("[{:<22}] Закрытие старого соединения (Graceful Close)...", tag);
                                let _ = tokio::time::timeout(Duration::from_secs(2), async {
                                    let _ = write.send(tokio_tungstenite::tungstenite::Message::Close(None)).await;
                                    let _ = write.flush().await;
                                }).await;

                                let (new_write, new_read) = new_stream.split();
                                write = new_write;
                                read = new_read;
                                uptime_start = Instant::now();
                                info!("[{:<22}] Переключение завершено! Старый сокет закрыт, счетчик аптайма сброшен.", tag);
                            }
                            Ok(Err(e)) => error!("[{:<22}] Ошибка Warmup: {}", tag, e),
                            Err(e) => error!("[{:<22}] Warmup panic: {}", tag, e),
                        }
                    }
                }

                ws_msg = tokio::time::timeout(Duration::from_secs(WS_TIMEOUT_SEC), read.next()) => {
                    match ws_msg {
                        Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) => {
                            parse_buf.clear(); parse_buf.extend_from_slice(text.as_bytes());
                            let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

                            if market == "FUTURES" {
                                if let Ok(p) = simd_json::from_slice::<BinanceMsgFutures>(&mut parse_buf) {
                                    if let Some((o, seq)) = local_registry.get_mut(p.data.s) {
                                        let lat = (ts as f64) - (p.data.e as f64);
                                        msg_count += 1; sum_latency += lat; if lat > max_latency { max_latency = lat; }
                                        unsafe { write_to_mmap(mmap_ptr.0, *o, seq, &p.data.b, &p.data.bq, &p.data.a, &p.data.aq, p.data.e); }
                                    }
                                }
                            } else {
                                if let Ok(p) = simd_json::from_slice::<BinanceMsgSpot>(&mut parse_buf) {
                                    if let Some((o, seq)) = local_registry.get_mut(p.data.s) {
                                        msg_count += 1;
                                        unsafe { write_to_mmap(mmap_ptr.0, *o, seq, &p.data.b, &p.data.bq, &p.data.a, &p.data.aq, ts); }
                                    }
                                }
                            }
                        }
                        Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Ping(_)))) => { let _ = write.flush().await; }
                        Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Close(_)))) | Ok(None) | Ok(Some(Err(_))) | Err(_) => {
                            warn!("[{:<22}] Внезапный разрыв. Принудительный реконнект...", tag);
                            break;
                        }
                        _ => {}
                    }
                }
            }

            if last_report.elapsed().as_secs() >= STATS_REPORT_INTERVAL_SEC {
                if msg_count > 0 {
                    let mps = msg_count as f64 / last_report.elapsed().as_secs_f64();
                    let avg_lat = sum_latency / msg_count as f64;
                    let up_m = uptime_start.elapsed().as_secs() / 60;

                    if market == "FUTURES" && max_latency > LATENCY_CRITICAL_MS {
                        warn!(
                            "[{:<22}] Up: {:>3}m | MPS: {:>7.1} | LatAvg: {:>5.1}ms | Max: {:>6.1}ms | Symbols: {}", 
                            tag, up_m, mps, avg_lat, max_latency, local_registry.len()
                        );
                    } else {
                        info!(
                            "[{:<22}] Up: {:>3}m | MPS: {:>7.1} | LatAvg: {:>5.1}ms | Max: {:>6.1}ms | Symbols: {}", 
                            tag, up_m, mps, avg_lat, max_latency, local_registry.len()
                        );
                    }
                } else {
                    info!("[{:<22}] Тишина (0 сообщений за {}с)", tag, STATS_REPORT_INTERVAL_SEC);
                }
                msg_count = 0; max_latency = 0.0; sum_latency = 0.0; last_report = Instant::now();
            }
        }
    }
}

// ==========================================
// 6. ПЛАНИРОВЩИК РЕКОННЕКТОВ (Orchestrator Scheduler)
// ==========================================
async fn scheduled_reconnector(all_txs: Vec<mpsc::Sender<WorkerCmd>>) {
    let interval = Duration::from_secs(SCHEDULED_RECONNECT_INTERVAL_SEC);
    
    loop {
        info!("[{:<22}] Ожидание 1 час до следующего цикла реконнектов...", "SCHEDULER");
        sleep(interval).await;

        info!("[{:<22}] Запуск каскадного планового переподключения для {} воркеров...", "SCHEDULER", all_txs.len());
        
        for (i, tx) in all_txs.iter().enumerate() {
            let _ = tx.send(WorkerCmd::PlannedReconnect).await;
            if i < all_txs.len() - 1 {
                // Роллинг апдейт с задержкой между переподключениями соседних воркеров
                sleep(Duration::from_secs(STREAMS_RESUBSCRIBE_DELAY_SEC)).await;
            }
        }
        
        info!("[{:<22}] Цикл переподключения завершен.", "SCHEDULER");
    }
}

// ==========================================
// 7. ОРКЕСТРАТОР РЕСТ И СИСТЕМНЫЕ ФУНКЦИИ
// ==========================================
async fn orchestrator(
    market: String,
    rest_url: String,
    num_workers: usize,
    worker_txs: Vec<mpsc::Sender<WorkerCmd>>,
    mmap_ptr: MmapPtr,
    skip_symbols: Vec<String>,
    starting_index: usize,
) {
    let client = Client::new();
    let mut symbol_to_index: FxHashMap<String, usize> = FxHashMap::default();
    let mut worker_symbols: Vec<FxHashMap<String, usize>> = vec![FxHashMap::default(); num_workers];
    let mut free_indices: Vec<usize> = Vec::new();
    let mut next_index = starting_index; 
    let tag = format!("{}-ORCH", market);

    loop {
        match client.get(&rest_url).send().await {
            Ok(resp) => {
                if let Ok(bytes) = resp.bytes().await {
                    let mut bytes_vec = bytes.to_vec();
                    if let Ok(data) = simd_json::from_slice::<ExchangeInfo>(&mut bytes_vec) {
                        let mut latest_symbols = Vec::new();
                        for s in data.symbols {
                            if skip_symbols.contains(&s.symbol) { continue; }
                            if (s.status == "TRADING" || s.status == "PRE_TRADING") && (s.symbol.ends_with("USDT") || s.symbol.ends_with("USDC")) {
                                latest_symbols.push(s.symbol);
                            }
                        }

                        let mut current_set = std::collections::HashSet::new();
                        for sym in symbol_to_index.keys() { current_set.insert(sym.clone()); }

                        let mut new_listings = Vec::new();
                        let latest_set: std::collections::HashSet<String> = latest_symbols.iter().cloned().collect();

                        for sym in &latest_symbols { if !current_set.contains(sym) { new_listings.push(sym.clone()); } }
                        let mut delistings = Vec::new();
                        for sym in &current_set { if !latest_set.contains(sym) { delistings.push(sym.clone()); } }

                        if !delistings.is_empty() {
                            for sym in &delistings {
                                if let Some(idx) = symbol_to_index.remove(sym) {
                                    free_indices.push(idx);
                                    unsafe { std::ptr::write_bytes(mmap_ptr.0.add(idx * RECORD_SIZE), 0, RECORD_SIZE); }
                                }
                                for (w_id, map) in worker_symbols.iter_mut().enumerate() {
                                    if map.remove(sym).is_some() {
                                        let _ = worker_txs[w_id].send(WorkerCmd::Unsubscribe(vec![sym.clone()])).await;
                                    }
                                }
                            }
                        }

                        if !new_listings.is_empty() {
                            let mut distribute = vec![Vec::new(); num_workers];
                            for sym in new_listings {
                                let idx = if let Some(f_idx) = free_indices.pop() { f_idx } else { let i = next_index; next_index += 1; i };
                                if idx >= MAX_COINS { continue; }
                                symbol_to_index.insert(sym.clone(), idx);
                                
                                let mut min_w = 0; let mut min_len = usize::MAX;
                                for (i, map) in worker_symbols.iter().enumerate() { if map.len() < min_len { min_len = map.len(); min_w = i; } }

                                worker_symbols[min_w].insert(sym.clone(), idx);
                                distribute[min_w].push((sym.clone(), idx * RECORD_SIZE));
                                unsafe {
                                    let base = mmap_ptr.0.add(idx * RECORD_SIZE);
                                    let s_bytes = sym.as_bytes();
                                    std::ptr::write_bytes(base.add(8), 0, 16);
                                    std::ptr::copy_nonoverlapping(s_bytes.as_ptr(), base.add(8), s_bytes.len().min(16));
                                }
                            }
                            for (i, payload) in distribute.into_iter().enumerate() {
                                if !payload.is_empty() { let _ = worker_txs[i].send(WorkerCmd::Subscribe(payload)).await; }
                            }
                        }
                    }
                }
            }
            Err(e) => error!("[{:<22}] Ошибка REST API: {}", tag, e),
        }
        sleep(Duration::from_secs(3600)).await;
    }
}

fn init_mmap(file_name: &str) -> MmapPtr {
    std::fs::create_dir_all(MMAP_DIR).unwrap();
    let file = OpenOptions::new().read(true).write(true).create(true).open(&format!("{}/{}", MMAP_DIR, file_name)).unwrap();
    file.set_len(SHM_SIZE as u64).unwrap();
    let mut mmap = unsafe { MmapOptions::new().map_mut(&file).unwrap() };
    mmap.fill(0); 
    MmapPtr(Box::leak(Box::new(mmap)).as_mut_ptr())
}

async fn monitor_system_load() {
    let mut sys = System::new_all();
    let pid = sysinfo::get_current_pid().unwrap();
    loop {
        sleep(Duration::from_secs(60)).await;
        sys.refresh_processes(ProcessesToUpdate::All, true);
        if let Some(process) = sys.process(pid) {
            info!("[{:<22}] CPU: {:>5.1}% (от 1 ядра) | RAM: {:.1} MB", "MONITOR", process.cpu_usage(), process.memory() as f64 / 1024.0 / 1024.0);
        }
    }
}

#[tokio::main]
async fn main() {
    std::fs::create_dir_all(LOG_DIR).unwrap();
    let file_appender = tracing_appender::rolling::daily(LOG_DIR, "writer.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    
    // Формат логов выровнен для ровной табличности
    tracing_subscriber::fmt()
        .with_writer(non_blocking)
        .with_target(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_ansi(false)
        .init();

    info!("Запуск Binance Writer: Бесшовный реконнект (Graceful Drain) + Интервальный планировщик");

    let ptr_fut = init_mmap("binance_bbo_futum_mem.mmap");
    let ptr_spot = init_mmap("binance_bbo_spot_mem.mmap");

    let mut skip_fut = Vec::new();
    let mut skip_spot = Vec::new();
    let mut next_idx_fut = 0;
    let mut next_idx_spot = 0;

    let mut all_reconnect_txs = Vec::new();
    let mut fut_orchestrator_txs = Vec::new();
    let mut spot_orchestrator_txs = Vec::new();

    let dedicated_list: Vec<(&str, &str)> = vec![("BTCUSDT", "FUTURES"), ("ETHUSDT", "FUTURES")];

    for (sym, market) in dedicated_list {
        let (tx, rx) = mpsc::channel(10);
        all_reconnect_txs.push(tx); 

        if market == "FUTURES" {
            skip_fut.push(sym.to_string());
            unsafe { std::ptr::write_bytes(ptr_fut.0.add(next_idx_fut * RECORD_SIZE).add(8), 0, 16);
                     std::ptr::copy_nonoverlapping(sym.as_bytes().as_ptr(), ptr_fut.0.add(next_idx_fut * RECORD_SIZE).add(8), sym.len().min(16)); }
            tokio::spawn(dedicated_ws_worker("FUTURES", sym.to_string(), "wss://fstream.binance.com/stream".to_string(), ptr_fut, next_idx_fut, rx));
            next_idx_fut += 1;
        } else if market == "SPOT" {
            skip_spot.push(sym.to_string());
            unsafe { std::ptr::write_bytes(ptr_spot.0.add(next_idx_spot * RECORD_SIZE).add(8), 0, 16);
                     std::ptr::copy_nonoverlapping(sym.as_bytes().as_ptr(), ptr_spot.0.add(next_idx_spot * RECORD_SIZE).add(8), sym.len().min(16)); }
            tokio::spawn(dedicated_ws_worker("SPOT", sym.to_string(), "wss://stream.binance.com:9443/stream".to_string(), ptr_spot, next_idx_spot, rx));
            next_idx_spot += 1;
        }
    }

    for i in 0..NUM_WORKERS_FUTURES {
        let (tx, rx) = mpsc::channel(100);
        fut_orchestrator_txs.push(tx.clone());
        all_reconnect_txs.push(tx);
        tokio::spawn(ws_worker("FUTURES", i, "wss://fstream.binance.com/stream".to_string(), ptr_fut, rx));
    }

    for i in 0..NUM_WORKERS_SPOT {
        let (tx, rx) = mpsc::channel(100);
        spot_orchestrator_txs.push(tx.clone());
        all_reconnect_txs.push(tx);
        tokio::spawn(ws_worker("SPOT", i, "wss://stream.binance.com:9443/stream".to_string(), ptr_spot, rx));
    }

    tokio::spawn(orchestrator("FUTURES".to_string(), "https://fapi.binance.com/fapi/v1/exchangeInfo".to_string(), NUM_WORKERS_FUTURES, fut_orchestrator_txs, ptr_fut, skip_fut, next_idx_fut));
    tokio::spawn(orchestrator("SPOT".to_string(), "https://api.binance.com/api/v3/exchangeInfo".to_string(), NUM_WORKERS_SPOT, spot_orchestrator_txs, ptr_spot, skip_spot, next_idx_spot));
    
    tokio::spawn(scheduled_reconnector(all_reconnect_txs));
    
    tokio::spawn(monitor_system_load());

    tokio::signal::ctrl_c().await.unwrap();
    info!("Выход.");
}