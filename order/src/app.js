const express = require("express");
const mongoose = require("mongoose");
const Order = require("./models/order");
const amqp = require("amqplib");
const config = require("./config");

class App {
  constructor() {
    this.app = express();
    this.connectDB();
    this.setupOrderConsumer();
  }

  async connectDB() {
    await mongoose.connect(config.mongoURI, {
      useNewUrlParser: true,
      useUnifiedTopology: true,
    });
    console.log("MongoDB connected");
  }

  async disconnectDB() {
    await mongoose.disconnect();
    console.log("MongoDB disconnected");
  }

  async setupOrderConsumer() {
    console.log("🔄 [Order] Connecting to RabbitMQ...");
  
    setTimeout(async () => {
      try {
        console.log("🔗 [Order] Attempting connection to:", config.rabbitMQURI);
        const connection = await amqp.connect(config.rabbitMQURI);
        console.log("✅ [Order] Connected to RabbitMQ");
        
        const channel = await connection.createChannel();
        console.log("📡 [Order] Channel created");
        
        await channel.assertQueue(config.rabbitMQQueue);
        console.log("🗂️ [Order] Queue asserted:", config.rabbitMQQueue);

        console.log("👂 [Order] Starting to consume messages from queue:", config.rabbitMQQueue);
        
        channel.consume(config.rabbitMQQueue, async (data) => {
          // Consume messages from the order queue on buy
          console.log("📨 [Order] Consuming ORDER service - message received");
          
          try {
            const messageContent = data.content.toString();
            console.log("📋 [Order] Raw message:", messageContent);
            
            const { products, username, orderId } = JSON.parse(messageContent);
            console.log("✅ [Order] Message parsed - OrderID:", orderId, "User:", username, "Products:", products.length);

            const newOrder = new Order({
              products,
              user: username,
              totalPrice: products.reduce((acc, product) => acc + product.price, 0),
            });

            // Save order to DB
            await newOrder.save();
            console.log("💾 [Order] Order saved to DB with ID:", newOrder._id);

            // Send ACK to ORDER service
            channel.ack(data);
            console.log("✅ [Order] ACK sent to ORDER queue");

            // Ensure products queue exists before sending
            await channel.assertQueue("products");
            console.log("🗂️ [Order] Products queue asserted");

            // Send fulfilled order to PRODUCTS service
            const { user, products: savedProducts, totalPrice } = newOrder.toJSON();
            const responseMessage = { orderId, user, products: savedProducts, totalPrice };
            
            console.log("📤 [Order] Sending fulfilled order to PRODUCTS queue");
            console.log("📋 [Order] Response message:", JSON.stringify(responseMessage, null, 2));
            
            channel.sendToQueue(
              "products",
              Buffer.from(JSON.stringify(responseMessage))
            );
            
            console.log("✅ [Order] Fulfilled order sent to PRODUCTS queue");
          } catch (error) {
            console.error("❌ [Order] Error processing message:", error);
            channel.reject(data, false);
          }
        });
      } catch (err) {
        console.error("❌ [Order] Failed to connect to RabbitMQ:", err.message);
      }
    }, 10000); // add a delay to wait for RabbitMQ to start in docker-compose
  }

  start() {
    this.server = this.app.listen(config.port, () =>
      console.log(`Server started on port ${config.port}`)
    );
  }

  async stop() {
    await mongoose.disconnect();
    this.server.close();
    console.log("Server stopped");
  }
}

module.exports = App;
