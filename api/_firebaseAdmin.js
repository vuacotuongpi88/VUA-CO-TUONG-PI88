const admin = require("./_firebaseAdmin.js")

const projectId = process.env.FIREBASE_PROJECT_ID || "";
const clientEmail = process.env.FIREBASE_CLIENT_EMAIL || "";
const privateKey = (process.env.FIREBASE_PRIVATE_KEY || "").replace(/\\n/g, "\n");
const databaseURL = process.env.FIREBASE_DATABASE_URL || "";

if (!projectId || !clientEmail || !privateKey || !databaseURL) {
  throw new Error(
    "Missing Firebase env: " +
    JSON.stringify({
      FIREBASE_PROJECT_ID: !!projectId,
      FIREBASE_CLIENT_EMAIL: !!clientEmail,
      FIREBASE_PRIVATE_KEY: !!privateKey,
      FIREBASE_DATABASE_URL: !!databaseURL
    })
  );
}

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert({
      projectId,
      clientEmail,
      privateKey
    }),
    databaseURL
  });
}

module.exports = admin;