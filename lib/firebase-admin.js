const admin = require("firebase-admin");

function getServiceAccount() {
  const raw =
    process.env.FIREBASE_SERVICE_ACCOUNT_JSON ||
    process.env.FIREBASE_SERVICE_ACCOUNT ||
    process.env.FIREBASE_ADMIN_JSON;

  if (!raw) {
    throw new Error("Thiếu FIREBASE_SERVICE_ACCOUNT_JSON trên Vercel.");
  }

  const serviceAccount = JSON.parse(raw);

  if (serviceAccount.private_key) {
    serviceAccount.private_key = serviceAccount.private_key.replace(/\\n/g, "\n");
  }

  return serviceAccount;
}

function getAdminDb() {
  if (!admin.apps.length) {
    const serviceAccount = getServiceAccount();

    const databaseURL =
      process.env.FIREBASE_DATABASE_URL ||
      process.env.NEXT_PUBLIC_FIREBASE_DATABASE_URL ||
      serviceAccount.databaseURL;

    if (!databaseURL) {
      throw new Error("Thiếu FIREBASE_DATABASE_URL trên Vercel.");
    }

    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
      databaseURL
    });
  }

  return admin.database();
}

module.exports = {
  admin,
  getAdminDb
};