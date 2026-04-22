module.exports = async function handler(req, res) {
  return res.status(200).json({
    ok: true,
    route: "withdraw-probe-v3",
    withdrawId: "probe_withdraw_001",
    txid: "probe_txid_ok",
    amount: 1,
    newBalance: 19,
    leftToday: 2
  });
};