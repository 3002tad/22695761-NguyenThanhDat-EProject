const Product = require("../models/product");
const messageBroker = require("../utils/messageBroker");
const uuid = require('uuid');

/**
 * Class to hold the API implementation for the product services
 */
class ProductController {

  constructor() {
    this.createOrder = this.createOrder.bind(this);
    this.getOrderStatus = this.getOrderStatus.bind(this);
    this.ordersMap = new Map();
    this.consumerSetup = false; // Track if consumer is already setup
  }

  async createProduct(req, res, next) {
    try {
      const token = req.headers.authorization;
      if (!token) {
        return res.status(401).json({ message: "Unauthorized" });
      }
      const product = new Product(req.body);

      const validationError = product.validateSync();
      if (validationError) {
        return res.status(400).json({ message: validationError.message });
      }

      await product.save({ timeout: 30000 });

      res.status(201).json(product);
    } catch (error) {
      console.error(error);
      res.status(500).json({ message: "Server error" });
    }
  }

  async createOrder(req, res, next) {
    try {
      const token = req.headers.authorization;
      if (!token) {
        return res.status(401).json({ message: "Unauthorized" });
      }
  
      const { ids } = req.body;
      console.log("🛒 [Product] Creating order for product IDs:", ids);
      
      const products = await Product.find({ _id: { $in: ids } });
      console.log("📦 [Product] Found products:", products.length);

      if (products.length === 0) {
        return res.status(404).json({ message: "No products found" });
      }
  
      const orderId = uuid.v4();
      console.log("🆔 [Product] Generated order ID:", orderId);
      
      this.ordersMap.set(orderId, { 
        status: "pending", 
        products, 
        username: req.user.username
      });
      console.log("📝 [Product] Order stored in memory with status: pending");

      // Setup consumer BEFORE publishing to avoid race condition
      console.log("👂 [Product] Setting up consumer for products queue (if not already setup)");
      if (!this.consumerSetup) {
        this.consumerSetup = true;
        messageBroker.consumeMessage("products", (data) => {
          console.log("📨 [Product] Received response from products queue:", data);
          const orderData = JSON.parse(JSON.stringify(data));
          const { orderId: responseOrderId } = orderData;
          
          if (responseOrderId) {
            const order = this.ordersMap.get(responseOrderId);
            if (order) {
              console.log("🔄 [Product] Updating order status to completed for ID:", responseOrderId);
              this.ordersMap.set(responseOrderId, { ...order, ...orderData, status: 'completed' });
              console.log("✅ [Product] Order updated:", { ...order, ...orderData, status: 'completed' });
            }
          }
        });
      }

      // Wait a bit for consumer setup, then publish
      await new Promise(resolve => setTimeout(resolve, 1000));

      console.log("📤 [Product] Publishing order to RabbitMQ orders queue");
      await messageBroker.publishMessage("orders", {
        products,
        username: req.user.username,
        orderId,
      });
  
      console.log("⏳ [Product] Starting long polling for order completion...");
      // Long polling until order is completed
      let order = this.ordersMap.get(orderId);
      let pollCount = 0;
      
      while (order.status !== 'completed' && pollCount < 30) { // Max 30 seconds
        await new Promise(resolve => setTimeout(resolve, 1000));
        order = this.ordersMap.get(orderId);
        pollCount++;
        console.log(`🔄 [Product] Polling attempt ${pollCount}/30 - Status: ${order.status}`);
      }
  
      if (order.status === 'completed') {
        console.log("🎉 [Product] Order completed successfully!");
        return res.status(201).json(order);
      } else {
        console.log("⏰ [Product] Order timeout - returning current status");
        return res.status(202).json({ message: "Order processing", orderId, status: order.status });
      }
    } catch (error) {
      console.error("❌ [Product] Error in createOrder:", error);
      res.status(500).json({ message: "Server error" });
    }
  }
  

  async getOrderStatus(req, res, next) {
    const { orderId } = req.params;
    const order = this.ordersMap.get(orderId);
    if (!order) {
      return res.status(404).json({ message: 'Order not found' });
    }
    return res.status(200).json(order);
  }

  async getProducts(req, res, next) {
    try {
      const token = req.headers.authorization;
      if (!token) {
        return res.status(401).json({ message: "Unauthorized" });
      }
      const products = await Product.find({});

      res.status(200).json(products);
    } catch (error) {
      console.error(error);
      res.status(500).json({ message: "Server error" });
    }
  }
}

module.exports = ProductController;
