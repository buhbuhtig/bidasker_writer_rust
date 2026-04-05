curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

source "$HOME/.cargo/env"


sysctl -w net.core.rmem_max=16777216
sysctl -w net.ipv4.tcp_rmem="4096 87380 16777216"

echo "net.core.rmem_max = 16777216" >> /etc/sysctl.conf
echo "net.ipv4.tcp_rmem = 4096 87380 16777216" >> /etc/sysctl.conf
sysctl -p