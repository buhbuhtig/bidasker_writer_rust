#!/bin/bash

# --- НАСТРОЙКИ ---
SERVICE_NAME="binance-writer"
BINARY_NAME="bidasker_writer_rust" # Имя из Cargo.toml
INSTALL_DIR="/usr/local/bin"
APP_DIR="$(pwd)" # Папка, где лежит .env и откуда запускаем
USER_NAME="$(whoami)"

echo "🚀 Начинаем интеграцию $SERVICE_NAME в систему..."

# 1. Сборка проекта (опционально, если нужно обновлять бинарник)
echo "📦 Сборка release-версии..."
cargo build --release

# 2. Остановка старого сервиса, если он есть
echo "🛑 Остановка существующего сервиса (если есть)..."
sudo systemctl stop $SERVICE_NAME 2>/dev/null

# 3. Копирование бинарника
echo "🚚 Установка бинарника в $INSTALL_DIR..."
sudo cp target/release/$BINARY_NAME $INSTALL_DIR/

# 4. Проверка прав на папки из вашего кода
# Ваш код использует /bidasker_writer_rust/
sudo mkdir -p /bidasker_writer_rust/log /bidasker_writer_rust/mmap
sudo chown -R $USER_NAME:$USER_NAME /bidasker_writer_rust/

# 5. Создание файла сервиса
echo "⚙️ Создание systemd unit файла..."
sudo bash -c "cat <<EOF > /etc/systemd/system/$SERVICE_NAME.service
[Unit]
Description=Binance Writer Rust Service
After=network.target

[Service]
Type=simple
User=$USER_NAME
WorkingDirectory=$APP_DIR
ExecStart=$INSTALL_DIR/$BINARY_NAME
Restart=always
RestartSec=5
# Ограничение ресурсов (опционально)
# MemoryMax=1G
# CPUQuota=50%

[Install]
WantedBy=multi-user.target
EOF"

# 6. Перезапуск демона и активация
echo "🔄 Переинициализация systemd..."
sudo systemctl daemon-reload
sudo systemctl enable $SERVICE_NAME
sudo systemctl start $SERVICE_NAME

echo "✅ Готово! Сервис запущен."
echo "📜 Перечитать .env без пересборки: sudo systemctl restart $SERVICE_NAME"
echo "📜 Проверить статус: systemctl status $SERVICE_NAME"
echo "🔍 Посмотреть логи: journalctl -u $SERVICE_NAME -f"