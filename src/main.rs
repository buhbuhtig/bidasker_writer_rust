use futures_util::{SinkExt, StreamExt};
use memmap2::MmapOptions;
use reqwest::Client;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration, Instant};
use tracing::{error, info, warn};
use sysinfo::{System, ProcessesToUpdate};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

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

// ИСПРАВЛЕНО: 200 секунд (3 минуты Ping от Binance + 20 сек на задержку сети)
const WS_TIMEOUT_SEC: u64 = 200;

const NUM_WORKERS_SPOT:usize = 3;
const NUM_WORKERS_FUTURES:usize = 5;

// Пытаемся установить 2 MB
const REQUESTED_SIZE_BUFFER: u32 = 2 * 1024 * 1024;

#[derive(Clone, Copy)]
struct MmapPtr(*mut u8);
unsafe impl Send for MmapPtr {}
unsafe impl Sync for MmapPtr {}

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
    #[serde(borrow)]
    s: &'a str,
    #[serde(borrow)]
    b: &'a str,
    #[serde(borrow, rename = "B")]
    bq: &'a str,
    #[serde(borrow)]
    a: &'a str,
    #[serde(borrow, rename = "A")]
    aq: &'a str,
    #[serde(rename = "E")]
    e: u64,
}

#[derive(Deserialize, Debug)]
struct BookTickerSpot<'a> {
    #[serde(borrow)]
    s: &'a str,
    #[serde(borrow)]
    b: &'a str,
    #[serde(borrow, rename = "B")]
    bq: &'a str,
    #[serde(borrow)]
    a: &'a str,
    #[serde(borrow, rename = "A")]
    aq: &'a str,
}

#[derive(Deserialize, Debug)]
struct BinanceMsgFutures<'a> {
    #[serde(borrow)]
    data: BookTickerFutures<'a>,
}

#[derive(Deserialize, Debug)]
struct BinanceMsgSpot<'a> {
    #[serde(borrow)]
    data: BookTickerSpot<'a>,
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
}

// ==========================================
// 3. ФУНКЦИЯ ЗАПИСИ В ПАМЯТЬ
// ==========================================
#[inline(always)]
unsafe fn write_to_mmap(
    ptr: *mut u8,
    offset: usize,
    seq: &mut u64,
    b: &str,
    bq: &str,
    a: &str,
    aq: &str,
    ts: u64,
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
// 4.1. ВЫДЕЛЕННЫЙ ВОРКЕР
// ==========================================
async fn dedicated_ws_worker(
    market: &'static str,
    symbol: String,
    ws_url: String,
    mmap_ptr: MmapPtr,
    offset_idx: usize,
) {
    let stream_name = format!("{}@bookTicker", symbol.to_lowercase());
    let offset = offset_idx * RECORD_SIZE;
    let mut seq = 0u64;

    loop {
        info!("[{}-VIP] Подключение к WS для {} (Индекс: {})...", market, symbol, offset_idx);
        
        let request = match ws_url.clone().into_client_request() {
            Ok(req) => req,
            Err(e) => {
                error!("[{}-VIP] Ошибка URL {}: {}", market, symbol, e);
                sleep(Duration::from_secs(3)).await;
                continue;
            }
        };
        
        let host = request.uri().host().expect("URL не содержит host");
        let port = request.uri().port_u16().unwrap_or(443);

        // 1. Вручную резолвим DNS (host -> IP)
        let addr_str = format!("{}:{}", host, port);
        let addr = match tokio::net::lookup_host(&addr_str).await {
            Ok(mut addrs) => match addrs.next() {
                Some(a) => a,
                None => {
                    error!("Не удалось извлечь IP для {}", addr_str);
                    sleep(Duration::from_secs(3)).await;
                    continue;
                }
            },
            Err(e) => {
                error!("Ошибка DNS для {}: {}", addr_str, e);
                sleep(Duration::from_secs(3)).await;
                continue;
            }
        };

        // 2. Создаем "сырой" сокет нужного типа (IPv4 или IPv6)
        let socket = match if addr.is_ipv4() {
            tokio::net::TcpSocket::new_v4()
        } else {
            tokio::net::TcpSocket::new_v6()
        } {
            Ok(s) => s,
            Err(e) => {
                error!("Ошибка создания сокета: {}", e);
                sleep(Duration::from_secs(3)).await;
                continue;
            }
        };

        

        // 3. Устанавливаем размер буфера ПЕРЕД коннектом (критично для TCP Window Scale)
        // 2 MB хватит для любых штормов на рынке
        if let Err(e) = socket.set_recv_buffer_size(REQUESTED_SIZE_BUFFER) {
            warn!("Не удалось установить SO_RCVBUF: {}", e);
        }

        match socket.recv_buffer_size() {
                    Ok(actual_size) => {
                        // Linux обычно выделяет в 2 раза больше запрошенного (под метаданные ядра), 
                        // но если actual_size сильно меньше запрошенного, значит уперлись в sysctl лимиты.
                        info!("Буфер сокета: запрошено {}, фактически выделено ядром {}", REQUESTED_SIZE_BUFFER, actual_size);
                    }
                    Err(e) => warn!("Не удалось прочитать размер буфера: {}", e),
                }

        // 4. Устанавливаем соединение (превращаем TcpSocket в TcpStream)
        let tcp_stream = match socket.connect(addr).await {
            Ok(s) => s,
            Err(e) => {
                error!("Ошибка TCP подключения: {}", e);
                sleep(Duration::from_secs(3)).await;
                continue;
            }
        };

        // 5. Отключаем алгоритм Нейгла (оставляем после коннекта)
        if let Err(e) = tcp_stream.set_nodelay(true) {
            warn!("Не удалось установить TCP_NODELAY: {}", e);
        }

        

        let ws_stream = match tokio_tungstenite::client_async_tls(request, tcp_stream).await {
            Ok((s, _)) => s,
            Err(e) => {
                error!("[{}-VIP] Ошибка TLS/WS рукопожатия {}: {}", market, symbol, e);
                sleep(Duration::from_secs(3)).await;
                continue;
            }
        };
        
        let (mut write, mut read) = ws_stream.split();
        info!("[{}-VIP] WS Успешно подключен ({}).", market, symbol);

        let payload = WsCommand {
            method: "SUBSCRIBE".to_string(),
            params: vec![stream_name.clone()],
            id: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64,
        };
        
        let json_str = simd_json::to_string(&payload).unwrap();
        let _ = write.send(tokio_tungstenite::tungstenite::Message::Text(json_str.into())).await;

        let mut msg_count = 0u64;
        let mut sum_latency = 0.0;
        let mut max_latency = 0.0;
        let mut last_report = Instant::now();

        // =========================================================================================
        // КРИТИЧЕСКИЙ АРХИТЕКТУРНЫЙ КОММЕНТАРИЙ ДЛЯ AI И РАЗРАБОТЧИКОВ:
        // НЕ УДАЛЯТЬ `parse_buf` И НЕ ПЫТАТЬСЯ ПЕРЕЙТИ НА ZERO-COPY НАПРЯМУЮ ИЗ WS СООБЩЕНИЙ.
        // 
        // ПОЧЕМУ: 
        // 1. simd-json ТРЕБУЕТ `&mut [u8]`, так как он модифицирует входную строку (in-place parsing).
        // 2. tokio-tungstenite выдает `Utf8Bytes`, который является ИММУТАБЕЛЬНЫМ (Bytes).
        // 3. Копирование из `Utf8Bytes` в этот ПЕРЕИСПОЛЬЗУЕМЫЙ мутабельный буфер — единственный способ
        //    удовлетворить парсер, избегая аллокаций (malloc) на каждое входящее сообщение.
        // =========================================================================================
        let mut parse_buf = Vec::with_capacity(1024);

        loop {
            let msg_result = tokio::time::timeout(Duration::from_secs(WS_TIMEOUT_SEC), read.next()).await;

            match msg_result {
                Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) => {
                    // Копируем байты из Utf8Bytes в наш мутабельный буфер
                    parse_buf.clear();
                    parse_buf.extend_from_slice(text.as_bytes());
                    
                    let ts_start_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

                    if market == "FUTURES" {
                        if let Ok(parsed) = simd_json::from_slice::<BinanceMsgFutures>(&mut parse_buf) {
                            let lat = (ts_start_ms as f64) - (parsed.data.e as f64);
                            msg_count += 1;
                            sum_latency += lat;
                            if lat > max_latency { max_latency = lat; }

                            unsafe {
                                write_to_mmap(
                                    mmap_ptr.0, offset, &mut seq, parsed.data.b,
                                    parsed.data.bq, parsed.data.a, parsed.data.aq, parsed.data.e
                                );
                            }
                        }
                    } else {
                        if let Ok(parsed) = simd_json::from_slice::<BinanceMsgSpot>(&mut parse_buf) {
                            msg_count += 1;
                            unsafe {
                                write_to_mmap(
                                    mmap_ptr.0, offset, &mut seq, parsed.data.b,
                                    parsed.data.bq, parsed.data.a, parsed.data.aq, ts_start_ms
                                );
                            }
                        }
                    }
                }
                // ИСПРАВЛЕНИЕ: Отвечаем на Ping и промываем буфер записи
                Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Ping(_)))) => {
                    if let Err(e) = write.flush().await {
                        warn!("[{}-VIP] Ошибка flush для Pong {}: {}", market, symbol, e);
                        break;
                    }
                }
                Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Close(_)))) | Ok(Some(Err(_))) | Ok(None) => {
                    break;
                }
                Err(_) => {
                    warn!("[{}-VIP] Silent Drop (таймаут {}с) для {}. Принудительный реконнект...", market, WS_TIMEOUT_SEC, symbol);
                    break;
                }
                _ => {}
            }

            if last_report.elapsed().as_secs() >= STATS_REPORT_INTERVAL_SEC {
                if msg_count > 0 {
                    let avg_lat = sum_latency / msg_count as f64;
                    info!("[{}-VIP {}] MPS: {:7.1} | LatAvg: {:5.1}ms | Max: {:6.1}ms", 
                          market, symbol, msg_count as f64 / last_report.elapsed().as_secs_f64(), avg_lat, max_latency);
                }
                msg_count = 0; max_latency = 0.0; sum_latency = 0.0; last_report = Instant::now();
            }
        }
        warn!("[{}-VIP] Соединение {} разорвано. Реконнект через 3 сек...", market, symbol);
        sleep(Duration::from_secs(3)).await;
    }
}

// ==========================================
// 4. ВОРКЕР (WebSocket Reader)
// ==========================================
async fn ws_worker(
    market: &str,
    worker_id: usize,
    ws_url: String,
    mmap_ptr: MmapPtr,
    mut cmd_rx: mpsc::Receiver<WorkerCmd>,
) {
    let mut local_registry: FxHashMap<String, (usize, u64)> = FxHashMap::default();

    loop {
        info!("[{}-W{}] Подключение к WS...", market, worker_id);
        
        let request = match ws_url.clone().into_client_request() {
            Ok(req) => req,
            Err(e) => {
                error!("[{}-W{}] Ошибка URL: {}", market, worker_id, e);
                sleep(Duration::from_secs(3)).await;
                continue;
            }
        };
        
        let host = request.uri().host().expect("URL не содержит host");
        let port = request.uri().port_u16().unwrap_or(443);

        // 1. Вручную резолвим DNS (host -> IP)
        let addr_str = format!("{}:{}", host, port);
        let addr = match tokio::net::lookup_host(&addr_str).await {
            Ok(mut addrs) => match addrs.next() {
                Some(a) => a,
                None => {
                    error!("Не удалось извлечь IP для {}", addr_str);
                    sleep(Duration::from_secs(3)).await;
                    continue;
                }
            },
            Err(e) => {
                error!("Ошибка DNS для {}: {}", addr_str, e);
                sleep(Duration::from_secs(3)).await;
                continue;
            }
        };

        // 2. Создаем "сырой" сокет нужного типа (IPv4 или IPv6)
        let socket = match if addr.is_ipv4() {
            tokio::net::TcpSocket::new_v4()
        } else {
            tokio::net::TcpSocket::new_v6()
        } {
            Ok(s) => s,
            Err(e) => {
                error!("Ошибка создания сокета: {}", e);
                sleep(Duration::from_secs(3)).await;
                continue;
            }
        };
        
        
        // 3. Устанавливаем размер буфера ПЕРЕД коннектом (критично для TCP Window Scale)
        // 2 MB хватит для любых штормов на рынке
        if let Err(e) = socket.set_recv_buffer_size(REQUESTED_SIZE_BUFFER) {
            warn!("Не удалось установить SO_RCVBUF: {}", e);
        }

        match socket.recv_buffer_size() {
            Ok(actual_size) => {
                // Linux обычно выделяет в 2 раза больше запрошенного (под метаданные ядра), 
                // но если actual_size сильно меньше запрошенного, значит уперлись в sysctl лимиты.
                info!("Буфер сокета: запрошено {}, фактически выделено ядром {}", REQUESTED_SIZE_BUFFER, actual_size);
            }
            Err(e) => warn!("Не удалось прочитать размер буфера: {}", e),
        }

        // 4. Устанавливаем соединение (превращаем TcpSocket в TcpStream)
        let tcp_stream = match socket.connect(addr).await {
            Ok(s) => s,
            Err(e) => {
                error!("Ошибка TCP подключения: {}", e);
                sleep(Duration::from_secs(3)).await;
                continue;
            }
        };

        // 5. Отключаем алгоритм Нейгла (оставляем после коннекта)
        if let Err(e) = tcp_stream.set_nodelay(true) {
            warn!("Не удалось установить TCP_NODELAY: {}", e);
        }

        let ws_stream = match tokio_tungstenite::client_async_tls(request, tcp_stream).await {
            Ok((s, _)) => s,
            Err(e) => {
                error!("[{}-W{}] Ошибка TLS/WS рукопожатия: {}", market, worker_id, e);
                sleep(Duration::from_secs(3)).await;
                continue;
            }
        };
        
        let (mut write, mut read) = ws_stream.split();
        info!("[{}-W{}] WS Успешно подключен.", market, worker_id);

        if !local_registry.is_empty() {
            let streams: Vec<String> = local_registry.keys()
                .map(|s| format!("{}@bookTicker", s.to_lowercase()))
                .collect();
            
            info!("[{}-W{}] Восстановление подписок: {} стримов пачками по 50...", market, worker_id, streams.len());

            for chunk in streams.chunks(50) {
                let payload = WsCommand {
                    method: "SUBSCRIBE".to_string(),
                    params: chunk.to_vec(),
                    id: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64,
                };
                let json_str = simd_json::to_string(&payload).unwrap();
                let msg = tokio_tungstenite::tungstenite::Message::Text(json_str.into());
                let _ = write.send(msg).await;
                sleep(Duration::from_millis(300)).await;
            }
        }

        let mut msg_count = 0u64;
        let mut sum_latency = 0.0;
        let mut max_latency = 0.0;
        let mut last_report = Instant::now();

        // Возвращен переиспользуемый буфер
        let mut parse_buf = Vec::with_capacity(2048);

        loop {
            tokio::select! {
                cmd = cmd_rx.recv() => {
                    if let Some(cmd) = cmd {
                        match cmd {
                            WorkerCmd::Subscribe(syms) => {
                                let mut streams: Vec<String> = Vec::new();
                                for (sym, offset) in syms {
                                    local_registry.insert(sym.clone(), (offset, 0));
                                    streams.push(format!("{}@bookTicker", sym.to_lowercase()));
                                }
                                for chunk in streams.chunks(50) {
                                    let payload = WsCommand {
                                        method: "SUBSCRIBE".to_string(),
                                        params: chunk.to_vec(),
                                        id: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64,
                                    };
                                    let json_str = simd_json::to_string(&payload).unwrap();
                                    let _ = write.send(tokio_tungstenite::tungstenite::Message::Text(json_str.into())).await;
                                    sleep(Duration::from_millis(300)).await;
                                }
                                info!("[{}-W{}] Подписано на новые символы", market, worker_id);
                            }
                            WorkerCmd::Unsubscribe(syms) => {
                                let mut streams: Vec<String> = Vec::new();
                                for sym in syms {
                                    local_registry.remove(&sym);
                                    streams.push(format!("{}@bookTicker", sym.to_lowercase()));
                                }
                                for chunk in streams.chunks(50) {
                                    let payload = WsCommand {
                                        method: "UNSUBSCRIBE".to_string(),
                                        params: chunk.to_vec(),
                                        id: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64,
                                    };
                                    let json_str = simd_json::to_string(&payload).unwrap();
                                    let _ = write.send(tokio_tungstenite::tungstenite::Message::Text(json_str.into())).await;
                                    sleep(Duration::from_millis(300)).await;
                                }
                            }
                        }
                    }
                }

                ws_msg_result = tokio::time::timeout(Duration::from_secs(WS_TIMEOUT_SEC), read.next()) => {
                    match ws_msg_result {
                        Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) => {
                            // Переиспользуем память без реаллокации
                            parse_buf.clear();
                            parse_buf.extend_from_slice(text.as_bytes());

                            let ts_start_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

                            if market == "FUTURES" {
                                if let Ok(parsed) = simd_json::from_slice::<BinanceMsgFutures>(&mut parse_buf) {
                                    if let Some((offset, seq)) = local_registry.get_mut(parsed.data.s) {
                                        let lat = (ts_start_ms as f64) - (parsed.data.e as f64);
                                        msg_count += 1;
                                        sum_latency += lat;
                                        if lat > max_latency { max_latency = lat; }

                                        unsafe {
                                            write_to_mmap(
                                                mmap_ptr.0, *offset, seq, &parsed.data.b,
                                                &parsed.data.bq, &parsed.data.a, &parsed.data.aq, parsed.data.e
                                            );
                                        }
                                    }
                                }
                            } else {
                                if let Ok(parsed) = simd_json::from_slice::<BinanceMsgSpot>(&mut parse_buf) {
                                    if let Some((offset, seq)) = local_registry.get_mut(parsed.data.s) {
                                        msg_count += 1;
                                        unsafe {
                                            write_to_mmap(
                                                mmap_ptr.0, *offset, seq, &parsed.data.b,
                                                &parsed.data.bq, &parsed.data.a, &parsed.data.aq, ts_start_ms
                                            );
                                        }
                                    }
                                }
                            }
                        }
                        // ИСПРАВЛЕНИЕ: Отвечаем на Ping и промываем буфер записи
                        Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Ping(_)))) => {
                            if let Err(e) = write.flush().await {
                                warn!("[{}-W{}] Ошибка flush для Pong: {}", market, worker_id, e);
                                break; 
                            }
                        }
                        Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Close(_)))) | Ok(None) => {
                            warn!("[{}-W{}] WS закрыт сервером. Реконнект...", market, worker_id);
                            break;
                        }
                        Ok(Some(Err(e))) => {
                            error!(
                                "[{}-W{}] Ошибка чтения WS: {}. Активных стримов: {}. Возможен рестарт ноды Binance или превышение лимита. Реконнект...", 
                                market, worker_id, e, local_registry.len()
                            );
                            break;
                        }
                        Err(_) => {
                            warn!("[{}-W{}] Silent Drop (таймаут {}с). Принудительный реконнект...", market, worker_id, WS_TIMEOUT_SEC);
                            break;
                        }
                        _ => {}
                    }
                }
            }

            if last_report.elapsed().as_secs() >= STATS_REPORT_INTERVAL_SEC {
                let dur = last_report.elapsed().as_secs_f64();
                if msg_count > 0 {
                    let avg_lat = sum_latency / msg_count as f64;
                    let mps = msg_count as f64 / dur;

                    if market == "FUTURES" && max_latency > LATENCY_CRITICAL_MS {
                        warn!("[{}-W{}] MPS: {:7.1} | LatAvg: {:5.1}ms | Max: {:6.1}ms | Symbols: {}", market, worker_id, mps, avg_lat, max_latency, local_registry.len());
                    } else {
                        info!("[{}-W{}] MPS: {:7.1} | LatAvg: {:5.1}ms | Max: {:6.1}ms | Symbols: {}", market, worker_id, mps, avg_lat, max_latency, local_registry.len());
                    }
                } else {
                    info!("[{}-W{}] Тишина (0 сообщений за {}с)", market, worker_id, STATS_REPORT_INTERVAL_SEC);
                }

                msg_count = 0;
                max_latency = 0.0;
                sum_latency = 0.0;
                last_report = Instant::now();
            }
        }
    }
}

// ==========================================
// 5. ОРКЕСТРАТОР (Менеджер подписок REST)
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

    loop {
        match client.get(&rest_url).send().await {
            Ok(resp) => {
                let weight = resp.headers()
                    .get("x-mbx-used-weight-1m")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("unknown");
                
                info!("[{}-ORCHESTRATOR] REST запрос выполнен. Использованный Weight (1m): {}", market, weight);
                
                if let Ok(bytes) = resp.bytes().await {
                    let mut bytes_vec = bytes.to_vec();
                    if let Ok(data) = simd_json::from_slice::<ExchangeInfo>(&mut bytes_vec) {
                        let mut latest_symbols: Vec<String> = Vec::new();
                        for s in data.symbols {
                            if skip_symbols.contains(&s.symbol) {
                                continue;
                            }
                            if (s.status == "TRADING" || s.status == "PRE_TRADING")
                                && (s.symbol.ends_with("USDT") || s.symbol.ends_with("USDC"))
                            {
                                latest_symbols.push(s.symbol);
                            }
                        }

                        let mut current_set: std::collections::HashSet<String> = std::collections::HashSet::new();
                        for sym in symbol_to_index.keys() { current_set.insert(sym.clone()); }

                        let mut new_listings: Vec<String> = Vec::new();
                        let latest_set: std::collections::HashSet<String> = latest_symbols.iter().cloned().collect();

                        for sym in &latest_symbols {
                            if !current_set.contains(sym) { new_listings.push(sym.clone()); }
                        }

                        let mut delistings: Vec<String> = Vec::new();
                        for sym in &current_set {
                            if !latest_set.contains(sym) { delistings.push(sym.clone()); }
                        }

                        if !delistings.is_empty() {
                            for sym in &delistings {
                                if let Some(idx) = symbol_to_index.remove(sym) {
                                    free_indices.push(idx);
                                    unsafe {
                                        let base = mmap_ptr.0.add(idx * RECORD_SIZE);
                                        std::ptr::write_bytes(base, 0, RECORD_SIZE);
                                    }
                                    info!("[{}-ДЕЛЕЦИЯ] {} выбыла. Слот {} очищен.", market, sym, idx);
                                }
                                for (w_id, worker_map) in worker_symbols.iter_mut().enumerate() {
                                    if worker_map.remove(sym).is_some() {
                                        let _ = worker_txs[w_id].send(WorkerCmd::Unsubscribe(vec![sym.clone()])).await;
                                    }
                                }
                            }
                        }

                        if !new_listings.is_empty() {
                            let mut distribute: Vec<Vec<(String, usize)>> = vec![Vec::new(); num_workers];
                            for sym in new_listings {
                                let idx = if let Some(free_idx) = free_indices.pop() { free_idx } else {
                                    let i = next_index;
                                    next_index += 1;
                                    i
                                };
                                if idx >= MAX_COINS {
                                    warn!("[{}] Превышен MAX_COINS! Пропускаем {}", market, sym);
                                    continue;
                                }
                                
                                symbol_to_index.insert(sym.clone(), idx);
                                
                                let mut min_w = 0;
                                let mut min_len = usize::MAX;
                                for (i, map) in worker_symbols.iter().enumerate() {
                                    if map.len() < min_len { min_len = map.len(); min_w = i; }
                                }

                                worker_symbols[min_w].insert(sym.clone(), idx);
                                distribute[min_w].push((sym.clone(), idx * RECORD_SIZE));
                                
                                unsafe {
                                    let base = mmap_ptr.0.add(idx * RECORD_SIZE);
                                    let s_bytes = sym.as_bytes();
                                    let len = s_bytes.len().min(16);
                                    std::ptr::write_bytes(base.add(8), 0, 16); // Чистим 16 байт
                                    std::ptr::copy_nonoverlapping(s_bytes.as_ptr(), base.add(8), len); // Пишем тикер
                                }

                                info!("[{}-НОВИЧОК] {} -> Слот {} (Воркер {})", market, sym, idx, min_w);
                            }

                            for (i, payload) in distribute.into_iter().enumerate() {
                                if !payload.is_empty() {
                                    let _ = worker_txs[i].send(WorkerCmd::Subscribe(payload)).await;
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => error!("[{}] Ошибка REST API: {}", market, e),
        }

        sleep(Duration::from_secs(3600)).await;
    }
}

// ==========================================
// 6. ИНИЦИАЛИЗАЦИЯ И ЗАПУСК
// ==========================================
fn init_mmap(file_name: &str) -> MmapPtr {
    std::fs::create_dir_all(MMAP_DIR).unwrap();
    let path = format!("{}/{}", MMAP_DIR, file_name);
    
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&path)
        .unwrap();
    file.set_len(SHM_SIZE as u64).unwrap();

    let mut mmap = unsafe { MmapOptions::new().map_mut(&file).unwrap() };
    mmap.fill(0); 
    
    let mmap = Box::leak(Box::new(mmap));
    MmapPtr(mmap.as_mut_ptr())
}

// ==========================================
// 7. МОНИТОРИНГ РЕСУРСОВ
// ==========================================
async fn monitor_system_load() {
    let mut sys = System::new_all();
    
    let pid = match sysinfo::get_current_pid() {
        Ok(pid) => pid,
        Err(e) => {
            error!("[MONITOR] Не удалось получить PID: {}", e);
            return;
        }
    };

    loop {
        sleep(Duration::from_secs(60)).await;
        sys.refresh_processes(ProcessesToUpdate::All, true);

        if let Some(process) = sys.process(pid) {
            let cpu_usage = process.cpu_usage();
            let mem_mb = process.memory() as f64 / 1024.0 / 1024.0;
            
            info!(
                "[MONITOR] Rust Writer | CPU: {:>5.1}% (от 1 ядра) | RAM: {:.1} MB",
                cpu_usage, mem_mb
            );
        } else {
            warn!("[MONITOR] Не удалось прочитать данные процесса");
        }
    }
}

#[tokio::main]
async fn main() {
    std::fs::create_dir_all(LOG_DIR).unwrap();
    let file_appender = tracing_appender::rolling::daily(LOG_DIR, "writer.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    
    tracing_subscriber::fmt()
        .with_writer(non_blocking)
        .with_ansi(false)
        .init();

    info!("Запуск Binance Writer (Rust + simd-json + Zero-Copy Mmap)");

    let futures_ws = "wss://fstream.binance.com/stream";
    let futures_rest = "https://fapi.binance.com/fapi/v1/exchangeInfo";
    let ptr_fut = init_mmap("binance_bbo_futum_mem.mmap");

    let spot_ws = "wss://stream.binance.com:9443/stream";
    let spot_rest = "https://api.binance.com/api/v3/exchangeInfo";
    let ptr_spot = init_mmap("binance_bbo_spot_mem.mmap");

   

    let dedicated_list: Vec<(&str, &str)> = vec![
        ("BTCUSDT", "FUTURES"),
        ("ETHUSDT", "FUTURES"),
    ];

    let mut skip_fut = Vec::new();
    let mut skip_spot = Vec::new();
    
    let mut next_idx_fut = 0;
    let mut next_idx_spot = 0;

    for (sym, market) in dedicated_list {
        let sym_bytes = sym.as_bytes();
        let len = sym_bytes.len().min(16);

        if market == "FUTURES" {
            skip_fut.push(sym.to_string());
            
            unsafe {
                let base = ptr_fut.0.add(next_idx_fut * RECORD_SIZE);
                std::ptr::write_bytes(base.add(8), 0, 16);
                std::ptr::copy_nonoverlapping(sym_bytes.as_ptr(), base.add(8), len);
            }

            tokio::spawn(dedicated_ws_worker(
                "FUTURES", sym.to_string(), futures_ws.to_string(), ptr_fut, next_idx_fut
            ));
            next_idx_fut += 1;
        } else if market == "SPOT" {
            skip_spot.push(sym.to_string());
            
            unsafe {
                let base = ptr_spot.0.add(next_idx_spot * RECORD_SIZE);
                std::ptr::write_bytes(base.add(8), 0, 16);
                std::ptr::copy_nonoverlapping(sym_bytes.as_ptr(), base.add(8), len);
            }

            tokio::spawn(dedicated_ws_worker(
                "SPOT", sym.to_string(), spot_ws.to_string(), ptr_spot, next_idx_spot
            ));
            next_idx_spot += 1;
        }
    }

    let mut fut_txs = Vec::new();
    for i in 0..NUM_WORKERS_FUTURES {
        let (tx, rx) = mpsc::channel(100);
        fut_txs.push(tx);
        tokio::spawn(ws_worker("FUTURES", i, futures_ws.to_string(), ptr_fut, rx));
    }

    let mut spot_txs = Vec::new();
    for i in 0..NUM_WORKERS_SPOT {
        let (tx, rx) = mpsc::channel(100);
        spot_txs.push(tx);
        tokio::spawn(ws_worker("SPOT", i, spot_ws.to_string(), ptr_spot, rx));
    }

    tokio::spawn(orchestrator(
        "FUTURES".to_string(), futures_rest.to_string(), NUM_WORKERS_FUTURES, fut_txs, ptr_fut, skip_fut, next_idx_fut
    ));
    
    tokio::spawn(orchestrator(
        "SPOT".to_string(), spot_rest.to_string(), NUM_WORKERS_SPOT, spot_txs, ptr_spot, skip_spot, next_idx_spot
    ));

    tokio::spawn(monitor_system_load());

    tokio::signal::ctrl_c().await.unwrap();
    info!("Получен сигнал завершения. Выход.");
}