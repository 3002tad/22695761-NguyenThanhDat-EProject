require("dotenv").config();

module.exports = {
  port: process.env.PORT || 3001,
  mongoURI: process.env.MONGODB_PRODUCT_URI || "mongodb://localhost/products",
  rabbitMQURI: process.env.RABBITMQ_URI || "amqp://localhost",
  exchangeName: process.env.RABBITMQ_EXCHANGE || "products",
  queueName: process.env.RABBITMQ_QUEUE || "products_queue",
};
