const applicationDatabase = db.getSiblingDB("SplitPaymentDb");

applicationDatabase.createUser({
  user: process.env.MONGODB_APP_USERNAME,
  pwd: process.env.MONGODB_APP_PASSWORD,
  roles: [{ role: "readWrite", db: "SplitPaymentDb" }]
});