class UDPRelayServer {
    constructor(config) {
      this.config = config || {
        relay_port: parseInt(process.env.RELAY_PORT || "41234", 10),
        maxPacketSize: parseInt(process.env.MAX_PACKET_SIZE || "1500", 10),
        verbose: process.env.VERBOSE ? process.env.VERBOSE === "true" : true,
        maxClients: parseInt(process.env.MAX_CLIENTS || "100", 10),
        heartbeatInterval: parseInt(process.env.HEARTBEAT || "15000", 10), // 15s
        hangDetectionTimeout: parseInt(process.env.HANG_TIMEOUT || "60000", 10), // 60s
      };
      this.clients = new Map();
      this.socket = null;
      this.socketActive = false;
      this.shutdownRequested = false;
      this.heartbeatTimer = null;
      this.hangDetectionTimer = null;
      this.packetCount = 0;
      this.errorCount = 0;
      this.lastActivity = Date.now();
      this.lastHeartbeat = Date.now();
      this.isProcessingPacket = false;
      this.currentPacketInfo = null;
      this.restartCount = 0;
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
        
        // Start heartbeat monitoring
        this.startHeartbeat();
  
        const socket = await Bun.udpSocket({
          port: this.config.relay_port,
          socket: {
            data: (socket, data, port, address) => {
              const packetInfo = `${address}:${port} (${data?.byteLength || 0} bytes)`;
              
              try {
                this.lastActivity = Date.now();
                this.packetCount++;
                this.isProcessingPacket = true;
                this.currentPacketInfo = packetInfo;
                
                this.debug(`📥 Received packet #${this.packetCount} from ${packetInfo}`);
                
                // Add size validation before processing
                if (!data || data.byteLength === 0 || data.byteLength > this.config.maxPacketSize) {
                  this.debug(`⚠️ Dropped invalid packet from ${packetInfo}`);
                  this.isProcessingPacket = false;
                  this.currentPacketInfo = null;
                  return;
                }
                
                // Use immediate processing with timeout protection
                const processPacket = async () => {
                  try {
                    await this.handlePacketSafely(data, { address, port });
                  } catch (err) {
                    this.errorCount++;
                    this.log(`❌ Packet processing error #${this.errorCount}:`, err?.stack || err);
                  } finally {
                    this.isProcessingPacket = false;
                    this.currentPacketInfo = null;
                  }
                };
                
                // Set a timeout for packet processing
                const timeoutId = setTimeout(() => {
                  this.errorCount++;
                  this.log(`🚨 HANG DETECTED: Packet processing timeout for ${packetInfo}`);
                  this.isProcessingPacket = false;
                  this.currentPacketInfo = null;
                  
                  // Force restart if we're hanging too often
                  if (this.errorCount % 5 === 0) {
                    this.log(`🔄 Too many hangs detected, forcing restart...`);
                    this.forceRestart();
                  }
                }, 30000); // 30 second timeout per packet
                
                processPacket().finally(() => {
                  clearTimeout(timeoutId);
                });
                
              } catch (err) {
                this.errorCount++;
                this.log(`❌ Error in data event handler #${this.errorCount}:`, err?.stack || err);
                this.isProcessingPacket = false;
                this.currentPacketInfo = null;
              }
            },
  
            connect: () => this.debug("🔌 connect event"),
            drain: () => this.debug("🧯 drain event — socket is writable again"),
  
            error: (socket, error) => {
              this.errorCount++;
              console.error(`❌ UDP socket error #${this.errorCount}:`, error?.stack || error);
              
              // Don't restart if shutdown was requested
              if (this.shutdownRequested) {
                return;
              }
              
              // Force restart on socket error
              this.log("🔄 Socket error detected, forcing restart...");
              this.forceRestart();
            },
  
            close: () => {
              console.warn("🚪 UDP socket closed");
              this.socketActive = false;
            },
          },
        });
  
        this.socket = socket;
        this.socketActive = true;
        this.log(`🚀 UDP server is listening on port ${this.config.relay_port} (restart #${this.restartCount})`);
        this.log(`📊 Max clients: ${this.config.maxClients}, Max packet size: ${this.config.maxPacketSize}`);
        this.log(`⚙️ Hang detection: ${this.config.hangDetectionTimeout}ms, Heartbeat: ${this.config.heartbeatInterval}ms`);
      } catch (err) {
        this.errorCount++;
        this.log(`❌ Failed to start server (attempt #${this.restartCount}):`, err?.stack || err);
        
        if (!this.shutdownRequested) {
          // Aggressive restart - don't give up
          this.log("🔄 Will retry startup in 2 seconds...");
          setTimeout(() => {
            if (!this.shutdownRequested) {
              this.start().catch(e => this.log("Retry startup error:", e?.stack || e));
            }
          }, 2000);
        }
      }
    }

    forceRestart() {
      if (this.shutdownRequested) return;
      
      this.log(`🚨 FORCE RESTART initiated (restart #${this.restartCount}, errors: ${this.errorCount})`);
      
      try {
        this.stop();
      } catch (err) {
        this.log("❌ Error during force stop:", err?.stack || err);
      }
      
      // Immediate restart
      setTimeout(() => {
        if (!this.shutdownRequested) {
          this.restartCount++;
          this.start().catch(e => {
            this.log("❌ Force restart failed:", e?.stack || e);
            // If restart fails, try again after delay
            setTimeout(() => {
              if (!this.shutdownRequested) {
                this.start().catch(ee => this.log("❌ Retry restart failed:", ee?.stack || ee));
              }
            }, 5000);
          });
        }
      }, 1000);
    }
  
    stop() {
      try {
        this.shutdownRequested = true;
        
        // Stop heartbeat
        if (this.heartbeatTimer) {
          clearInterval(this.heartbeatTimer);
          this.heartbeatTimer = null;
        }
        
        if (this.socket) {
          this.log("🧹 Stopping UDP socket...");
          this.socket.close();
          this.socket = null;
        }
        
        this.clients.clear();
      } catch (err) {
        this.log("❌ Error during shutdown:", err?.stack || err);
      } finally {
        this.socketActive = false;
      }
    }

    startHeartbeat() {
      if (this.heartbeatTimer) {
        clearInterval(this.heartbeatTimer);
      }
      
      this.heartbeatTimer = setInterval(() => {
        const now = Date.now();
        const timeSinceActivity = now - this.lastActivity;
        const timeSinceHeartbeat = now - this.lastHeartbeat;
        
        this.lastHeartbeat = now;
        
        const status = {
          clients: this.clients.size,
          packets: this.packetCount,
          errors: this.errorCount,
          restarts: this.restartCount,
          lastActivity: `${timeSinceActivity}ms ago`,
          processing: this.isProcessingPacket ? `YES (${this.currentPacketInfo})` : 'NO'
        };
        
        this.log(`💓 Heartbeat:`, JSON.stringify(status));
        
        // Aggressive hang detection
        if (this.isProcessingPacket && this.currentPacketInfo) {
          const processingTime = now - this.lastActivity;
          if (processingTime > this.config.hangDetectionTimeout) {
            this.errorCount++;
            this.log(`🚨 HANG DETECTED: Processing packet for ${processingTime}ms: ${this.currentPacketInfo}`);
            this.isProcessingPacket = false;
            this.currentPacketInfo = null;
            
            // Force restart if hung
            this.forceRestart();
            return;
          }
        }
        
        // Clean up stale clients more aggressively
        const staleClients = [];
        for (const [client, lastSeen] of this.clients) {
          if (now - lastSeen > 60000) { // 1 minute instead of 2
            staleClients.push(client);
          }
        }
        
        if (staleClients.length > 0) {
          for (const client of staleClients) {
            this.clients.delete(client);
            this.log(`🗑️ Removed stale client: ${client}`);
          }
        }
        
        // Check for various hang conditions
        if (timeSinceActivity > 180000) { // 3 minutes of no activity
          this.log(`⚠️ No activity for ${timeSinceActivity}ms - checking server health...`);
          
          // If we have no activity and no clients, that's ok
          if (this.clients.size === 0) {
            this.log("ℹ️ No activity but no clients - server is idle (OK)");
          } else {
            this.log(`🚨 No activity but ${this.clients.size} clients connected - possible hang!`);
            this.forceRestart();
          }
        }
        
        // Check if socket became inactive unexpectedly
        if (!this.socketActive && !this.shutdownRequested) {
          this.errorCount++;
          this.log("🚨 Socket became inactive unexpectedly - restarting...");
          this.forceRestart();
        }
        
      }, this.config.heartbeatInterval);
    }

    async handlePacketSafely(data, rinfo) {
      const startTime = Date.now();
      try {
        await this.handlePacket(data, rinfo);
        const processingTime = Date.now() - startTime;
        if (processingTime > 1000) {
          this.log(`⚠️ Slow packet processing: ${processingTime}ms for ${rinfo.address}:${rinfo.port}`);
        }
      } catch (err) {
        this.errorCount++;
        const processingTime = Date.now() - startTime;
        this.log(`❌ Packet handling error #${this.errorCount} (${processingTime}ms):`, err?.stack || err);
        // Continue processing other packets - don't let one error stop everything
      }
    }
  
    async handlePacket(data, rinfo) {
      const sender = this.addrToStr(rinfo.address, rinfo.port);
      const now = Date.now();

      // Check client limit
      if (!this.clients.has(sender) && this.clients.size >= this.config.maxClients) {
        this.debug(`⚠️ Client limit reached, dropping packet from ${sender}`);
        return;
      }

      // Update client last seen time
      this.clients.set(sender, now);
      
      if (this.clients.size === 1) {
        this.debug(`👤 First client connected: ${sender}`);
      } else if (!this.clients.has(sender)) {
        this.debug(`👤 New client added: ${sender} (${this.clients.size} total)`);
      }

      const failed = new Set();
      const sendPromises = [];
      let relayCount = 0;

      for (const [client] of this.clients) {
        if (client === sender) continue;

        const { address, port } = this.strToAddr(client);
        
        // Create a promise for each send operation with additional safety
        const sendPromise = this.sendToClientSafely(data, address, port, client, sender)
          .then(() => {
            relayCount++;
          })
          .catch(err => {
            const reason = err?.message || err?.code || String(err);
            this.log(`❌ Send error to ${client}:`, reason);
            failed.add(client);
          });
        
        sendPromises.push(sendPromise);
      }

      // Wait for all send operations to complete (with aggressive timeout)
      try {
        const timeoutPromise = new Promise((resolve) => {
          setTimeout(() => {
            this.log(`⚠️ Send operations timeout after 5s for packet from ${sender}`);
            resolve('timeout');
          }, 5000); // 5 second timeout for all sends (reduced from 10s)
        });
        
        const result = await Promise.race([
          Promise.allSettled(sendPromises),
          timeoutPromise
        ]);
        
        if (result === 'timeout') {
          this.errorCount++;
          this.log(`🚨 Send timeout detected - may indicate network issues`);
        }
        
        if (relayCount > 0) {
          this.debug(`📤 Relayed ${data.byteLength} bytes from ${sender} to ${relayCount} clients`);
        }
        
      } catch (err) {
        this.errorCount++;
        this.log(`❌ Unexpected error in send operations #${this.errorCount}:`, err?.stack || err);
      }

      // Clean up clients that failed to receive
      if (failed.size > 0) {
        for (const client of failed) {
          this.clients.delete(client);
          this.log(`🗑️ Removed unreachable client: ${client}`);
        }
      }
    }

    async sendToClientSafely(data, address, port, client, sender) {
      // Multiple layers of validation
      if (!this.socketActive || !this.socket || this.shutdownRequested) {
        throw new Error('Socket inactive');
      }
      
      if (!data || data.byteLength === 0) {
        throw new Error('Invalid data');
      }
      
      if (!address || !port || port <= 0 || port > 65535) {
        throw new Error('Invalid address/port');
      }

      try {
        this.debug(`📤 Sending ${data.byteLength} bytes from ${sender} to ${client}`);
        
        // Create a copy of the data to prevent potential memory issues
        const dataCopy = new Uint8Array(data);
        
        // Wrap the send in a promise with timeout
        const sendOperation = new Promise((resolve, reject) => {
          try {
            // Validate socket one more time right before send
            if (!this.socket || !this.socketActive) {
              return reject(new Error('Socket became inactive'));
            }
            
            const result = this.socket.send(dataCopy, port, address);
            
            // Handle both sync and async returns
            if (result && typeof result.then === 'function') {
              result.then(resolve).catch(reject);
            } else {
              resolve(result);
            }
          } catch (err) {
            reject(err);
          }
        });
        
        const timeoutPromise = new Promise((_, reject) => {
          setTimeout(() => reject(new Error('Send timeout after 2s')), 2000); // Reduced from 3s
        });
        
        await Promise.race([sendOperation, timeoutPromise]);
        this.debug(`✅ Successfully sent to ${client}`);
        
      } catch (err) {
        // Enhanced error logging
        const errorInfo = {
          code: err?.code || err?.errno,
          message: err?.message || String(err),
          client,
          dataSize: data.byteLength
        };
        
        this.debug(`❌ Send failed to ${client}:`, JSON.stringify(errorInfo));
        throw err;
      }
    }
  }
  
  

  // --- Main ---
  
  // Enhanced signal handling with aggressive restart capability
  let relay;
  let watchdogTimer;
  let lastWatchdogCheck = Date.now();
  
  // Aggressive watchdog to detect and fix hangs
  const startWatchdog = () => {
    watchdogTimer = setInterval(() => {
      const now = Date.now();
      
      if (relay) {
        const timeSinceActivity = now - relay.lastActivity;
        const timeSinceHeartbeat = now - relay.lastHeartbeat;
        const isHung = relay.isProcessingPacket && timeSinceActivity > 30000;
        
        // Log watchdog status
        console.log(`🐕 Watchdog: Active=${relay.socketActive}, Processing=${relay.isProcessingPacket}, ` +
                   `Activity=${timeSinceActivity}ms, Heartbeat=${timeSinceHeartbeat}ms, Errors=${relay.errorCount}`);
        
        // Detect various hang conditions
        if (isHung) {
          console.log(`🚨 WATCHDOG: Hang detected! Processing for ${timeSinceActivity}ms`);
          relay.forceRestart();
        } else if (timeSinceHeartbeat > 120000) { // 2 minutes without heartbeat
          console.log(`🚨 WATCHDOG: Heartbeat stopped! Last: ${timeSinceHeartbeat}ms ago`);
          relay.forceRestart();
        } else if (!relay.socketActive && !relay.shutdownRequested) {
          console.log(`🚨 WATCHDOG: Socket inactive unexpectedly!`);
          relay.forceRestart();
        }
        
        // If too many errors, restart preventively
        if (relay.errorCount > 50 && (relay.errorCount % 25 === 0)) {
          console.log(`🚨 WATCHDOG: Too many errors (${relay.errorCount}), preventive restart`);
          relay.forceRestart();
        }
      } else {
        console.log(`🚨 WATCHDOG: No relay instance! Attempting to recreate...`);
        startServer().catch(e => console.error("Watchdog restart failed:", e));
      }
      
      lastWatchdogCheck = now;
    }, 15000); // Check every 15 seconds (more frequent)
  };
  
  const stopWatchdog = () => {
    if (watchdogTimer) {
      clearInterval(watchdogTimer);
      watchdogTimer = null;
    }
  };
  
  const gracefulShutdown = (signal) => {
    console.log(`🛑 ${signal} received, shutting down gracefully...`);
    stopWatchdog();
    
    if (relay) {
      try {
        relay.stop();
      } catch (err) {
        console.log("❌ Error during graceful shutdown:", err);
      }
    }
    
    // Force exit after timeout
    setTimeout(() => {
      console.log("⚠️ Force exit after timeout");
      process.exit(1);
    }, 3000);
    
    // Allow time for graceful shutdown
    setTimeout(() => {
      process.exit(0);
    }, 1000);
  };

  process.on("SIGINT", () => gracefulShutdown("SIGINT"));
  process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));

  // Aggressive error recovery - never give up!
  process.on("uncaughtException", (err) => {
    console.log("❗ CRITICAL - Uncaught exception:", err?.stack || err);
    
    // Log current state
    if (relay) {
      console.log(`📊 State: active=${relay.socketActive}, clients=${relay.clients?.size || 0}, ` +
                 `errors=${relay.errorCount}, processing=${relay.isProcessingPacket}`);
    }
    
    // Try to recover by restarting the server
    setTimeout(() => {
      console.log("🔄 Attempting recovery from uncaught exception...");
      if (relay) {
        relay.forceRestart();
      } else {
        startServer().catch(e => console.error("Recovery failed:", e));
      }
    }, 1000);
  });

  process.on("unhandledRejection", (err, promise) => {
    console.log("❗ CRITICAL - Unhandled promise rejection:", err?.stack || err);
    console.log("🔍 Promise details:", promise);
    
    // Log current state and continue
    if (relay) {
      console.log(`📊 State: active=${relay.socketActive}, clients=${relay.clients?.size || 0}, errors=${relay.errorCount}`);
      
      // If we're getting too many unhandled rejections, restart
      relay.errorCount += 5; // Count unhandled rejections as multiple errors
      if (relay.errorCount % 20 === 0) {
        console.log("🔄 Too many unhandled rejections, forcing restart...");
        relay.forceRestart();
      }
    }
  });

  // Catch fatal signals
  process.on("SIGSEGV", () => {
    console.log("💥 SEGMENTATION FAULT - Process will crash!");
    stopWatchdog();
    process.exit(139);
  });

  process.on("SIGABRT", () => {
    console.log("💥 ABORT SIGNAL - Process will crash!");
    stopWatchdog();
    process.exit(134);
  });

  process.on("exit", (code) => {
    stopWatchdog();
    console.log(`⚰️  Process exiting with code ${code} at ${new Date().toISOString()}`);
  });

  // Aggressive server startup with retry logic
  const startServer = async (attempt = 1) => {
    try {
      console.log(`🚀 Starting server (attempt #${attempt})...`);
      relay = new UDPRelayServer();
      
      if (attempt === 1) {
        startWatchdog();
      }
      
      await relay.start();
      console.log(`✅ Server started successfully on attempt #${attempt}`);
      
    } catch (err) {
      console.error(`🚨 Startup error on attempt #${attempt}:`, err?.stack || err);
      
      if (attempt < 10) { // Try up to 10 times
        const delay = Math.min(1000 * attempt, 10000); // Exponential backoff, max 10s
        console.log(`⏳ Retrying in ${delay}ms...`);
        
        setTimeout(() => {
          startServer(attempt + 1).catch(e => 
            console.error(`Failed retry #${attempt + 1}:`, e)
          );
        }, delay);
      } else {
        console.error("💀 All startup attempts failed, but keeping watchdog running...");
        // Keep the watchdog running to retry later
      }
    }
  };

  startServer();