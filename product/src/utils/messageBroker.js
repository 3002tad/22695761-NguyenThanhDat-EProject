const amqp = require("amqplib");
const config = require("../config");

class MessageBroker {
  constructor() {
    this.channel = null;
  }

  async connect() {
    console.log("🔄 [MessageBroker] Connecting to RabbitMQ...");

    setTimeout(async () => {
      try {
        console.log("🔗 [MessageBroker] Attempting connection to:", config.rabbitMQURI || "amqp://localhost");
        const connection = await amqp.connect(config.rabbitMQURI || "amqp://localhost");
        console.log("✅ [MessageBroker] Connected to RabbitMQ successfully");
        
        this.channel = await connection.createChannel();
        console.log("📡 [MessageBroker] Channel created");
        
        // Assert both queues exist
        await this.channel.assertQueue(config.queueName || "products");
        console.log("🗂️ [MessageBroker] Queue asserted:", config.queueName || "products");
        
        await this.channel.assertQueue("orders"); // For publishing to orders
        console.log("🗂️ [MessageBroker] Queue asserted: orders");
        
        console.log("🎉 [MessageBroker] RabbitMQ setup completed");
      } catch (err) {
        console.error("❌ [MessageBroker] Failed to connect to RabbitMQ:", err.message);
      }
    }, 20000); // delay 20 seconds to wait for RabbitMQ to start
  }

  async publishMessage(queue, message) {
    if (!this.channel) {
      console.error("❌ [MessageBroker] No RabbitMQ channel available for publishing");
      return;
    }

    try {
      console.log("📤 [MessageBroker] Publishing message to queue:", queue);
      console.log("📋 [MessageBroker] Message content:", JSON.stringify(message, null, 2));
      
      await this.channel.sendToQueue(
        queue,
        Buffer.from(JSON.stringify(message))
      );
      
      console.log("✅ [MessageBroker] Message published successfully to queue:", queue);
    } catch (err) {
      console.error("❌ [MessageBroker] Failed to publish message:", err);
    }
  }

  async consumeMessage(queue, callback) {
    if (!this.channel) {
      console.error("❌ [MessageBroker] No RabbitMQ channel available for consuming");
      return;
    }

    try {
      console.log("📥 [MessageBroker] Starting to consume messages from queue:", queue);
      
      // Ensure queue exists before consuming
      await this.channel.assertQueue(queue);
      console.log("🗂️ [MessageBroker] Queue asserted for consuming:", queue);
      
      await this.channel.consume(queue, (message) => {
        const content = message.content.toString();
        console.log("📨 [MessageBroker] Received message from queue:", queue);
        console.log("📋 [MessageBroker] Message content:", content);
        
        try {
          const parsedContent = JSON.parse(content);
          console.log("✅ [MessageBroker] Message parsed successfully");
          callback(parsedContent);
          this.channel.ack(message);
          console.log("✅ [MessageBroker] Message acknowledged");
        } catch (parseErr) {
          console.error("❌ [MessageBroker] Failed to parse message:", parseErr);
          this.channel.ack(message); // Still ack to remove from queue
        }
      });
      
      console.log("✅ [MessageBroker] Consumer setup completed for queue:", queue);
    } catch (err) {
      console.error("❌ [MessageBroker] Failed to setup consumer:", err);
    }
  }
}

module.exports = new MessageBroker();
