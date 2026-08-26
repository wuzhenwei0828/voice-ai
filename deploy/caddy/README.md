# Caddy 反向代理部署

## 为什么需要

浏览器要求 `navigator.mediaDevices.getUserMedia()`（麦克风权限）只能在 **secure context** 下授权：

- `https://` 任意域名 ✅
- `http://localhost` / `http://127.0.0.1` ✅
- `http://192.168.x.x` / `http://公网 IP` ❌（拒绝授权）

如果你直接 `cargo run` 起 voice_server 然后把 `http://<局域网 IP>:8080` 分享给别人，浏览器会拒绝麦克风权限。对应 Caddyfile 注释里那句"上一句话的文本还在推流"之前的根源问题。

Caddy 在前面做 TLS 终止：对外 `https://` → 反代到本机 `http://127.0.0.1:8080`，浏览器看到 https 就允许授权麦克风。

## 三种部署场景

### 场景 A：公网域名（推荐）

适用：有真实域名（如 `voice.example.com`）且 80/443 端口可被外网访问。

```bash
# 1. 编辑 Caddyfile，把 YOUR_DOMAIN 换成实际域名
$EDITOR Caddyfile

# 2. 跑 Caddy（Caddy 会自动从 Let's Encrypt 签证书 + 自动续期）
caddy run --config ./Caddyfile

# 3. 浏览器访问 https://voice.example.com
```

首次启动时 Caddy 会要求 ACME HTTP-01 challenge 验证域名所有权，需要 DNS 已指向本机。

### 场景 B：内网 / LAN（自签证书）

适用：无公网域名，需要局域网分享。

```bash
# 1. 编辑 Caddyfile，把场景 A 整段注释掉，把场景 B 反注释，把 {LAN_IP} 换成你的 IP
$EDITOR Caddyfile

# 2. 跑 Caddy（tls internal → Caddy 自己签发 self-signed 证书）
caddy run --config ./Caddyfile

# 3. 浏览器访问 https://192.168.x.x（首次会看到证书警告）
```

首次访问浏览器会显示 "您的连接不是私密连接"。**手动信任**：
- Chrome / Edge：地址栏点 "不安全" → "证书无效" → 切到 "详细信息" → "导出"（导出成 .crt），双击导入到"受信任的根证书颁发机构"，刷新页面
- Safari：弹出窗口点 "显示证书" → 勾选 "始终信任"，刷新
- Firefox：完全独立的证书库，Settings → Privacy & Security → Certificates → View Certificates → Import，勾选 "Trust this CA to identify websites"

**更好的方案**：用 [mkcert](https://github.com/FiloSottile/mkcert) 本地签发受信证书：

```bash
brew install mkcert
mkcert -install
mkcert 192.168.1.20 voice.local "*.voice.local"
# 生成 192.168.1.20+5.pem 和 192.168.1.20+5-key.pem
# 把 Caddyfile 改成 tls ./192.168.1.20+5.pem ./192.168.1.20+5-key.pem
```

### 场景 C：纯本地调试

不需要 Caddy。直接：

```bash
cargo run --release
# 浏览器访问 http://localhost:8080
```

`localhost` 是 secure context，麦克风权限直接能用。

## 验证

```bash
# voice_server 自带健康检查端点
curl http://127.0.0.1:8080/health

# 经过 Caddy 之后：
curl https://your-domain/health
```

打开浏览器 DevTools → Network → 刷新页面，应该能看到：
- `200` 状态码
- `WSS` 协议的 `/ws/voice/...` 连接成功建立（之前是 `WS`）
- 麦克风权限弹窗能正常出现

## 常见问题

### Q: 浏览器还是拒绝麦克风权限
检查：
1. DevTools Console 里有没有 `getUserMedia is not allowed in insecure context` —— 说明还在 http，把 Caddy 配好再来
2. 系统设置（macOS：系统设置 → 隐私与安全性 → 麦克风）里浏览器是否有权限

### Q: Caddy 启动报错 "binding to :443: permission denied"
Linux 上需要 root 或 `setcap cap_net_bind_service=+ep`：
```bash
sudo setcap cap_net_bind_service=+ep $(which caddy)
```
或者用非 443 端口（要改 Caddyfile）。

### Q: WebSocket 连不上
确认 voice_server 的 `cfg.server.port` 跟 Caddyfile 里 `reverse_proxy` 指向的端口一致（默认都是 8080）。

### Q: 想看 Caddy 的实时日志
```bash
caddy run --config ./Caddyfile  # 前台跑，日志直接打印
```
后台跑用 `caddy start` + `caddy logs`。

## 进一步

- voice_server 目前还在 HTTP。Caddy 反代后实际是 voice_server ← HTTP ← Caddy ← HTTPS ← 浏览器。如果你对延迟敏感（比如语音对话），可以把 voice_server 升级到直接 TLS（用 rustls 绑 443），省一跳，但这样就要把 TLS 证书的运维交给 voice_server 自己管。
- Caddy 还支持自动 HTTP/3（QUIC）、OCSP stapling、HTTP/2 push 等，对实时语音流改善有限，先不开。
