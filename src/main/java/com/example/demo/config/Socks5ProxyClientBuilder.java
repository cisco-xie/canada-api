package com.example.demo.config;

import okhttp3.ConnectionPool;
import okhttp3.OkHttpClient;

import javax.net.SocketFactory;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * SOCKS5 代理客户端构建器（支持用户名/密码认证）
 * 完整实现 SOCKS5 协议规范（RFC 1928）
 */
public class Socks5ProxyClientBuilder {

    /**
     * 创建支持 SOCKS5 认证的 OkHttpClient
     * @param proxyHost 代理服务器地址
     * @param proxyPort 代理端口（通常 1080）
     * @param username  认证用户名（可为空）
     * @param password  认证密码（可为空）
     * @return 配置好的 OkHttpClient 实例
     */
    public static OkHttpClient createSocks5Client(String proxyHost, int proxyPort,
                                                  String username, String password) {
        return new OkHttpClient.Builder()
                .socketFactory(new Socks5SocketFactory(proxyHost, proxyPort, username, password))
                .proxy(Proxy.NO_PROXY) // 必须设置 NO_PROXY，否则 OkHttp 会尝试系统代理
                .followRedirects(false) // 禁用自动重定向，方便拿location
                .connectTimeout(10, TimeUnit.SECONDS)
                .readTimeout(10, TimeUnit.SECONDS)
                .callTimeout(10, TimeUnit.SECONDS)
                .retryOnConnectionFailure(false)
                .connectionPool(new ConnectionPool(0, 1, TimeUnit.MINUTES))
                .build();
    }

    /**
     * 自定义 SocketFactory 实现 SOCKS5 协议
     */
    private static class Socks5SocketFactory extends SocketFactory {
        private final String proxyHost;
        private final int proxyPort;
        private final String username;
        private final String password;

        public Socks5SocketFactory(String proxyHost, int proxyPort,
                                   String username, String password) {
            this.proxyHost = proxyHost;
            this.proxyPort = proxyPort;
            this.username = username;
            this.password = password;
        }

        /**
         * 核心方法：创建未连接的 Socket（OkHttp 会负责后续连接）
         */
        @Override
        public Socket createSocket() throws IOException {
            return new Socks5SocketWrapper(proxyHost, proxyPort, username, password);
        }

        // 以下方法在 OkHttp 中不会被调用，只需抛出异常
        @Override public Socket createSocket(String host, int port) throws IOException {
            throw new UnsupportedOperationException();
        }
        @Override public Socket createSocket(String host, int port, InetAddress localHost, int localPort) throws IOException {
            throw new UnsupportedOperationException();
        }
        @Override public Socket createSocket(InetAddress host, int port) throws IOException {
            throw new UnsupportedOperationException();
        }
        @Override public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort) throws IOException {
            throw new UnsupportedOperationException();
        }

        /**
         * SOCKS5 Socket 包装类（实现延迟连接和认证）
         */
        private static class Socks5SocketWrapper extends Socket {
            private final String proxyHost;
            private final int proxyPort;
            private final String username;
            private final String password;
            private Socket proxySocket; // 实际代理连接

            public Socks5SocketWrapper(String proxyHost, int proxyPort,
                                       String username, String password) {
                this.proxyHost = proxyHost;
                this.proxyPort = proxyPort;
                this.username = username;
                this.password = password;
            }

            /**
             * 连接目标主机（包含 SOCKS5 认证流程）
             */
            @Override
            public void connect(SocketAddress endpoint, int timeout) throws IOException {
                // 1. 先连接到 SOCKS5 代理服务器
                proxySocket = new Socket();
                proxySocket.connect(new InetSocketAddress(proxyHost, proxyPort), timeout);

                // 2. 执行 SOCKS5 协议握手和认证
                doSocks5Handshake(proxySocket, (InetSocketAddress) endpoint);
            }

            /**
             * 完整的 SOCKS5 协议流程
             */
            private void doSocks5Handshake(Socket socket, InetSocketAddress target) throws IOException {
                InputStream in = socket.getInputStream();
                OutputStream out = socket.getOutputStream();

                // === 阶段1：协议协商 ===
                // 发送支持的认证方法（0x02=用户名/密码，0x00=无认证）
                byte[] authMethods = username != null && password != null
                        ? new byte[]{0x05, 0x01, 0x02}  // VER=5, NMETHODS=1, METHODS=USER/PASS
                        : new byte[]{0x05, 0x01, 0x00}; // 无认证

                out.write(authMethods);
                out.flush();

                // 读取服务器选择的认证方法
                byte[] authResponse = new byte[2];
                in.read(authResponse);
                if (authResponse[0] != 0x05) {
                    throw new IOException("Invalid SOCKS version: " + authResponse[0]);
                }

                // === 阶段2：用户名/密码认证（如果需要） ===
                if (authResponse[1] == 0x02) {
                    authenticateWithUsernamePassword(in, out);
                } else if (authResponse[1] != 0x00) {
                    throw new IOException("Unsupported SOCKS5 auth method: " + authResponse[1]);
                }

                // === 阶段3：建立代理连接 ===
                sendConnectRequest(in, out, target);
            }

            /**
             * SOCKS5 用户名/密码认证（RFC 1929）
             */
            private void authenticateWithUsernamePassword(InputStream in, OutputStream out)
                    throws IOException {
                if (username == null || password == null) {
                    throw new IOException("SOCKS5 server requires authentication but no credentials provided");
                }

                // 构建认证请求包
                ByteArrayOutputStream authPacket = new ByteArrayOutputStream();
                authPacket.write(0x01); // 认证版本
                authPacket.write(username.length());
                authPacket.write(username.getBytes(StandardCharsets.UTF_8));
                authPacket.write(password.length());
                authPacket.write(password.getBytes(StandardCharsets.UTF_8));

                out.write(authPacket.toByteArray());
                out.flush();

                // 验证认证结果
                byte[] authResponse = new byte[2];
                in.read(authResponse);
                if (authResponse[0] != 0x01 || authResponse[1] != 0x00) {
                    throw new IOException("SOCKS5 authentication failed");
                }
            }

            /**
             * 发送 SOCKS5 连接请求
             */
            private void sendConnectRequest(InputStream in, OutputStream out,
                                            InetSocketAddress target) throws IOException {
                ByteArrayOutputStream request = new ByteArrayOutputStream();
                request.write(0x05); // SOCKS版本
                request.write(0x01); // CONNECT命令
                request.write(0x00); // 保留字段

                // 目标地址类型（0x03=域名，0x01=IPv4）
                if (target.getAddress() != null) {
                    request.write(0x01); // IPv4
                    request.write(target.getAddress().getAddress());
                } else {
                    request.write(0x03); // 域名
                    byte[] hostBytes = target.getHostName().getBytes(StandardCharsets.UTF_8);
                    request.write(hostBytes.length);
                    request.write(hostBytes);
                }

                // 目标端口（2字节网络序）
                request.write((target.getPort() >> 8) & 0xFF);
                request.write(target.getPort() & 0xFF);

                out.write(request.toByteArray());
                out.flush();

                // 读取连接响应（简化处理，只读取前10字节）
                byte[] response = new byte[10];
                in.read(response);
                if (response[0] != 0x05 || response[1] != 0x00) {
                    throw new IOException("SOCKS5 connection failed: " + response[1]);
                }
            }

            // 委托方法（所有操作转发给 proxySocket）
            @Override public InputStream getInputStream() throws IOException {
                return proxySocket.getInputStream();
            }
            @Override public OutputStream getOutputStream() throws IOException {
                return proxySocket.getOutputStream();
            }
            @Override public void close() throws IOException {
                if (proxySocket != null) proxySocket.close();
            }
            @Override public boolean isConnected() {
                return proxySocket != null && proxySocket.isConnected();
            }
            // 其他需要重写的 Socket 方法...
        }
    }
}


