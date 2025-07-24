class UDPRelayServer {
    constructor(config) {
      this.config = config || {
        relay_port: parseInt(process.env.RELAY_PORT || "41234", 10),
        maxPacketSize: parseInt(process.env.MAX_PACKET_SIZE || "1500", 10),
        verbose: process.env.VERBOSE ? process.env.VERBOSE === "true" : true,
      };
      this.clients = new Set();
      this.socket = null;
      this.socketActive = false;
      this.shutdownRequested = false;
    }
  
    log(...args) {
      console.log(`[${new Date().toISOString()}]`, ...args);
    }
  
    debug(...args) {
      if (this.config.verbose) {
        this.log("🐛 DEBUG:", ...args);
      }
    }
  
    addrToStr(address, port) {
      return `${address}:${port}`;
    }
  
    strToAddr(str) {
      const i = str.lastIndexOf(":");
      return { address: str.slice(0, i), port: parseInt(str.slice(i + 1), 10) };
    }
  
    async start() {
      try {
        if (this.socket) {
          this.log("Server already running.");
          return;
        }
  
        this.shutdownRequested = false;
        this.log("Starting Bun UDP socket...");
  
        const socket = await Bun.udpSocket({
          port: this.config.relay_port,
          socket: {
            data: (socket, data, port, address) => {
              // Wrap in setImmediate to prevent blocking the event loop
              // and to ensure errors don't crash the socket handler
              setImmediate(() => {
                this.handlePacketSafely(data, { address, port })
                  .catch(err => {
                    this.log("❌ Critical error in packet handler:", err?.stack || err);
                    // Don't let packet handling errors crash the server
                  });
              });
            },
  
            connect: () => this.debug("🔌 connect event"),
            drain: () => this.debug("🧯 drain event — socket is writable again"),
  
            error: (socket, error) => {
              console.error("❌ UDP socket error:", error?.stack || error);
              
              // Don't restart if shutdown was requested
              if (this.shutdownRequested) {
                return;
              }
              
              if (this.socketActive) {
                this.stop();
                console.log("⏳ Restarting server in 5 seconds...");
                setTimeout(() => {
                  if (!this.shutdownRequested) {
                    this.start().catch((e) => this.log("Restart error:", e?.stack || e));
                  }
                }, 5000);
              }
            },
  
            close: () => {
              console.warn("🚪 UDP socket closed");
              this.socketActive = false;
            },
          },
        });
  
        this.socket = socket;
        this.socketActive = true;
        this.log(`🚀 UDP server is listening on port ${this.config.relay_port}`);
      } catch (err) {
        this.log("❌ Failed to start server:", err?.stack || err);
        if (!this.shutdownRequested) {
          process.exit(1);
        }
      }
    }
  
    stop() {
      try {
        this.shutdownRequested = true;
        if (this.socket) {
          this.log("🧹 Stopping UDP socket...");
          this.socket.close();
          this.socket = null;
        }
      } catch (err) {
        this.log("❌ Error during shutdown:", err?.stack || err);
      } finally {
        this.socketActive = false;
      }
    }

    async handlePacketSafely(data, rinfo) {
      try {
        await this.handlePacket(data, rinfo);
      } catch (err) {
        this.log("❌ Packet handling error:", err?.stack || err);
        // Log but don't crash - continue processing other packets
      }
    }
  
    async handlePacket(data, rinfo) {
      const sender = this.addrToStr(rinfo.address, rinfo.port);

      if (!data || data.byteLength === 0 || data.byteLength > this.config.maxPacketSize) {
        this.debug(`⚠️ Dropped invalid packet from ${sender}`);
        return;
      }

      if (!this.clients.has(sender)) {
        this.clients.add(sender);
        this.debug(`👤 New client added: ${sender} (${this.clients.size})`);
      }

      const failed = new Set();
      const sendPromises = [];

      for (const client of this.clients) {
        if (client === sender) continue;

        const { address, port } = this.strToAddr(client);
        
        // Create a promise for each send operation
        const sendPromise = this.sendToClient(data, address, port, client, sender)
          .catch(err => {
            const reason = err?.message || err?.code || err;
            this.log(`❌ Send error to ${client}:`, reason);
            failed.add(client);
            // Don't rethrow - we handle failed sends by removing clients
          });
        
        sendPromises.push(sendPromise);
      }

      // Wait for all send operations to complete
      try {
        await Promise.allSettled(sendPromises);
      } catch (err) {
        // This shouldn't happen since we catch errors in sendToClient,
        // but adding extra safety
        this.log("❌ Unexpected error in send operations:", err?.stack || err);
      }

      // Clean up clients that failed to receive
      if (failed.size > 0) {
        for (const client of failed) {
          this.clients.delete(client);
          this.log(`🗑️ Removed unreachable client: ${client}`);
        }
      }
    }

    async sendToClient(data, address, port, client, sender) {
      // Double-check socket state before sending
      if (!this.socketActive || !this.socket || this.shutdownRequested) {
        this.debug(`❌ Socket inactive — skipping send to ${client}`);
        throw new Error('Socket inactive');
      }

      try {
        this.debug(`📤 Will Relay ${data.byteLength} bytes from ${sender} to ${client}`);
        
        // Add timeout protection for send operation
        const sendPromise = this.socket.send(data, port, address);
        const timeoutPromise = new Promise((_, reject) => {
          setTimeout(() => reject(new Error('Send timeout')), 5000);
        });
        
        await Promise.race([sendPromise, timeoutPromise]);
        this.debug(`📤 Relayed ${data.byteLength} bytes to ${client}`);
        
      } catch (err) {
        // Be more specific about error types
        const errorCode = err?.code || err?.errno || 'UNKNOWN';
        const errorMessage = err?.message || String(err);
        
        this.debug(`❌ Send failed to ${client} [${errorCode}]: ${errorMessage}`);
        
        // Re-throw so the caller can handle it
        throw err;
      }
    }
  }
  
  

  // --- Main ---
  let relay;
  
  // Enhanced signal handling
  const gracefulShutdown = (signal) => {
    console.log(`🛑 ${signal} received, shutting down gracefully...`);
    if (relay) {
      relay.stop();
    }
    
    // Force exit after timeout
    setTimeout(() => {
      console.log("⚠️ Force exit after timeout");
      process.exit(1);
    }, 2000);
    
    // Allow time for graceful shutdown
    setTimeout(() => {
      process.exit(0);
    }, 500);
  };

  process.on("SIGINT", () => gracefulShutdown("SIGINT"));
  process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));

  process.on("uncaughtException", (err) => {
    console.log("❗ Uncaught exception:", err?.stack || err);
    console.log("🔄 Attempting to continue...");
    // Don't exit immediately - let the application try to recover
  });

  process.on("unhandledRejection", (err) => {
    console.log("❗ Unhandled promise rejection:", err?.stack || err);
    console.log("🔄 Attempting to continue...");
    // Don't exit immediately - let the application try to recover
  });

  process.on("exit", (code) => {
    console.log(`⚰️  Process exiting with code ${code}`);
  });

  // Start the server with better error handling
  const startServer = async () => {
    try {
      relay = new UDPRelayServer();
      await relay.start();
    } catch (err) {
      console.error("🚨 Fatal startup error:", err?.stack || err);
      process.exit(1);
    }
  };

  startServer();