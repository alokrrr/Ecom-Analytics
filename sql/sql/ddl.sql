cat > sql/ddl.sql <<'SQL'
-- ddl.sql
CREATE SCHEMA IF NOT EXISTS ecom;

CREATE TABLE IF NOT EXISTS ecom.users (
  user_id INTEGER PRIMARY KEY,
  email VARCHAR(255) UNIQUE,
  signup_date DATE,
  country VARCHAR(100),
  user_type VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS ecom.products (
  product_id INTEGER PRIMARY KEY,
  sku VARCHAR(64) UNIQUE,
  name TEXT,
  description TEXT,
  category VARCHAR(100),
  price NUMERIC(10,2),
  created_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ecom.orders (
  order_id INTEGER PRIMARY KEY,
  user_id INT NOT NULL REFERENCES ecom.users(user_id),
  order_date TIMESTAMP NOT NULL,
  status VARCHAR(50),
  total_amount NUMERIC(12,2)
);

CREATE TABLE IF NOT EXISTS ecom.order_items (
  order_item_id INTEGER PRIMARY KEY,
  order_id INT NOT NULL REFERENCES ecom.orders(order_id),
  product_id INT NOT NULL REFERENCES ecom.products(product_id),
  quantity INT,
  unit_price NUMERIC(10,2)
);

CREATE TABLE IF NOT EXISTS ecom.product_reviews (
  review_id INTEGER PRIMARY KEY,
  product_id INT REFERENCES ecom.products(product_id),
  user_id INT REFERENCES ecom.users(user_id),
  rating SMALLINT,
  review_text TEXT,
  review_date TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_orders_order_date ON ecom.orders(order_date);
CREATE INDEX IF NOT EXISTS idx_orders_user_id ON ecom.orders(user_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON ecom.order_items(product_id);
CREATE INDEX IF NOT EXISTS idx_products_category ON ecom.products(category);
SQL
