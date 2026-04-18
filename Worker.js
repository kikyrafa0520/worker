// ================= FULL MANUAL + AUTO SMART INVENTORY GRID DGBIDR V3.5 =================
//
// PATCH V3.5:
// - Semua logic V3.4 tetap
// - Tambah RANGE DINAMIS via Telegram
// - Range disimpan di KV STATE
// - Auto inventory target ikut range aktif terbaru
// - Command baru:
//   /range
//   /setrange 40 100
//
// IMPORTANT ENV:
// BOT_TOKEN
// CHAT_ID
// TRADING_ENABLED=true|false
// INDODAX_KEY
// INDODAX_SECRET
// STATE (KV binding)
//
// CRON EXAMPLE:
// */5 * * * *

const HEARTBEAT_INTERVAL_SEC = 360000;

const GRID_PAIR_PUBLIC = "DGBIDR";
const GRID_PAIR_TAPI = "dgb_idr";

const IDR_ROUND_TO = 1000;

const LAST_PRICE_STATE_KEY = "LAST_PRICE_STATE_V35";
const FILL_RECENCY_SEC = 2 * 3600;
const RUN_LOCK_KEY = "RUN_LOCK_FULL_MANUAL_DGBIDR_V35";
const RUN_LOCK_SEC = 20;

const STATE_KEY = "FULL_MANUAL_STATE_DGBIDR_V35";
const ASSUMED_ENTRY_PRICE_DEFAULT = 68;

// ================= DEFAULT INVENTORY GRID SETTINGS =================
const DEFAULT_GRID_RANGE_LOW = 50;
const DEFAULT_GRID_RANGE_HIGH = 100;
const DEFAULT_ACTIVE_RANGE = 14;
const DEFAULT_BUY_NEAR_GAP = 2;
const DEFAULT_SELL_NEAR_GAP = 2;
const MAX_CANCEL_PER_RUN = 10;
const MAX_NEW_BUY_PER_RUN = 4;
const MAX_NEW_SELL_PER_RUN = 4;
const MIN_IDR_RESERVE = 400000;
const MIN_DGB_RESERVE = 2500;
const INVENTORY_BIAS_TOLERANCE = 0.05;

// ================= VOLUME CONFIRMATION SETTINGS =================
const MIN_VOLUME_24H_IDR = 1000000; // Minimum 24h volume in IDR for pair

// ================= AUTO INVENTORY TARGET SETTINGS =================
const AUTO_TARGET_MAX_RATIO = 0.86; // level harga terendah
const AUTO_TARGET_MIN_RATIO = 0.14; // level harga tertinggi

// ================= REBALANCE SETTINGS =================
const REBALANCE_ENABLED = true;
const REBALANCE_MAX_BUY_PER_RUN = 2;
const REBALANCE_MAX_SELL_PER_RUN = 2;
const REBALANCE_NEAR_COUNT = 3;
const REBALANCE_DIFF_PCT = 0.25; // 25%
const REBALANCE_ONLY_ON_BIAS_CHANGE = true;

// ================= SIDEWAYS MICRO GRID SETTINGS =================
const SIDEWAYS_LOOKBACK_SEC = 6 * 3600;
const SIDEWAYS_MIN_TRADES = 10;
const SIDEWAYS_MAX_RANGE_PCT = 0.035;
const SIDEWAYS_CENTER_TOLERANCE = 0.60;

const DEFAULT_MICRO_ACTIVE_RANGE = 6;
const MICRO_MAX_NEW_BUY_PER_RUN = 2;
const MICRO_MAX_NEW_SELL_PER_RUN = 2;
const MICRO_NEAR_GAP = 0;
const MICRO_SIZE_FACTOR = 0.65;
const MICRO_MIN_SPREAD_PCT = 0.018;

// pair static tetap dipakai untuk manual mode / seed / lock compatibility
function generateDynamicPairs(low, high, spread = 2) {
  const pairs = [];
  for (let buy = low; buy < high; buy += spread) {
    const sell = buy + spread;
    if (sell <= high + spread) { // allow slight over for flexibility
      pairs.push([buy, sell]);
    }
  }
  return pairs;
}

const PAIRS = generateDynamicPairs(DEFAULT_GRID_RANGE_LOW, DEFAULT_GRID_RANGE_HIGH);

const BUY_TO_SELL = new Map(PAIRS.map(([b, s]) => [b, s]));
const SELL_TO_BUY = new Map(PAIRS.map(([b, s]) => [s, b]));
const PAIR_BUY_MIN = 40; // Fixed min for safety
const PAIR_BUY_MAX = 200; // Fixed max for safety

// Function to update pair maps when range changes
function updatePairMaps(low, high) {
  const dynamicPairs = generateDynamicPairs(low, high);
  BUY_TO_SELL.clear();
  SELL_TO_BUY.clear();
  for (const [b, s] of dynamicPairs) {
    BUY_TO_SELL.set(b, s);
    SELL_TO_BUY.set(s, b);
  }
  // Update min/max if needed, but for now keep static
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/") {
      return new Response("Full Manual + Auto Smart Inventory Grid DGBIDR V3.5 Running", { status: 200 });
    }

    if (url.pathname === "/webhook" && request.method === "POST") {
      const update = await request.json();
      await handleTelegram(env, update);
      return new Response("OK", { status: 200 });
    }

    return new Response("Not Found", { status: 404 });
  },

  async scheduled(event, env, ctx) {
    ctx.waitUntil(runOnce(env, env.CHAT_ID, false));
  }
};

// ================= TELEGRAM =================
async function handleTelegram(env, update) {
  if (!update.message) return;

  const chatId = update.message.chat.id;
  const text0 = (update.message.text || "").trim();
  const text = text0.toLowerCase();

  if (text === "/grid") {
    await runOnce(env, chatId, true);
    return;
  }

  if (text === "/range") {
    const st = await getState(env);
    ensureStateShape(st);
    const cfg = getRuntimeConfig(st);

    await sendMsg(
      env,
      chatId,
      `📐 RANGE AKTIF\n\n` +
      `gridRange   : ${cfg.gridRangeLow}-${cfg.gridRangeHigh}\n` +
      `activeRange : ±${cfg.activeRange}\n` +
      `microRange  : ±${cfg.microActiveRange}\n` +
      `buyNearGap  : ${cfg.buyNearGap}\n` +
      `sellNearGap : ${cfg.sellNearGap}\n\n` +
      `Range akan ikut harga otomatis saat harga menyentuh tepi.\n` +
      `Ubah range manual:\n` +
      `/setrange 40 100`
    );
    return;
  }

  if (text.startsWith("/setrange ")) {
    const parts = text0.split(/\s+/);
    const low = Math.round(Number(parts[1]));
    const high = Math.round(Number(parts[2]));

    if (!Number.isFinite(low) || !Number.isFinite(high) || low <= 0 || high <= 0 || low >= high) {
      await sendMsg(env, chatId, "Format: /setrange 40 100");
      return;
    }

    const hasBuyInRange = PAIRS.some(([b]) => b >= low && b <= high);
    if (!hasBuyInRange) {
      await sendMsg(env, chatId, `⛔ Range ${low}-${high} tidak punya BUY level yang valid di PAIRS.`);
      return;
    }

    const st = await getState(env);
    ensureStateShape(st);

    st.gridRangeLow = low;
    st.gridRangeHigh = high;

    await setState(env, st);

    await sendMsg(
      env,
      chatId,
      `✅ RANGE DIUBAH\n\n` +
      `gridRange: ${low}-${high}\n` +
      `Auto inventory target sekarang akan mengikuti range ini.\n` +
      `Cek dengan /grid atau /range`
    );
    return;
  }

  if (text === "/status") {
    const st = await getState(env);
    ensureStateShape(st);
    const cfg = getRuntimeConfig(st);
    const envEnabled = String(env.TRADING_ENABLED || "").toLowerCase() === "true";
    const tradingActive = envEnabled && !st.manualStopped && !st.manualPaused;

    await sendMsg(
      env,
      chatId,
      `🧾 STATUS FULL MANUAL + AUTO GRID DGBIDR V3.5\n` +
      `mode           : FULL MANUAL + AUTO INVENTORY + MICRO SIDEWAYS\n` +
      `assumedEntry   : ${st.assumedEntryPrice}\n` +
      `manualPaused   : ${st.manualPaused ? "YES" : "NO"}\n` +
      `manualStopped  : ${st.manualStopped ? "YES" : "NO"}\n` +
      `envEnabled     : ${envEnabled ? "true" : "false"}\n` +
      `tradingActive  : ${tradingActive ? "YES ✅" : "NO ⛔"}\n` +
      `autoGrid       : ${st.autoGridEnabled ? "ON ✅" : "OFF ⛔"}\n` +
      `lastBias       : ${st.lastBias || "-"}\n` +
      `lastTradeTs    : ${st.lastTradeTs}\n` +
      `lastTradeSig   : ${st.lastTradeSig || "-"}\n` +
      `pairStateCount : ${Object.keys(st.pairSide || {}).length}\n` +
      `Locked BUY     : ${formatBuyLocks(st)}\n` +
      `Locked SELL    : ${formatSellLocks(st)}\n` +
      `lastReportTs   : ${st.lastReportTs}\n\n` +
      `Auto settings:\n` +
      `range          : ${cfg.gridRangeLow}-${cfg.gridRangeHigh}\n` +
      `activeRange    : ±${cfg.activeRange}\n` +
      `microRange     : ±${cfg.microActiveRange}\n` +
      `idrReserve     : ${MIN_IDR_RESERVE.toLocaleString("id-ID")}\n` +
      `dgbReserve     : ${fmtQty(MIN_DGB_RESERVE)}\n` +
      `autoRatioRange : ${(AUTO_TARGET_MAX_RATIO * 100).toFixed(1)}% -> ${(AUTO_TARGET_MIN_RATIO * 100).toFixed(1)}%\n` +
      `rebalance      : ${REBALANCE_ENABLED ? "ON ✅" : "OFF ⛔"}\n` +
      `rebDiffPct     : ${(REBALANCE_DIFF_PCT * 100).toFixed(0)}%\n` +
      `microMinSpread : ${(MICRO_MIN_SPREAD_PCT * 100).toFixed(2)}%\n\n` +
      `Tools:\n` +
      `/grid | /status | /range | /pairs | /locks | /orders | /ordersraw | /reset\n` +
      `/setrange 40 100\n` +
      `/autogrid on | /autogrid off\n` +
      `/assume 68 | /seedbuy 68 | /seedsell 70\n` +
      `/lockbuy 68 | /unlockbuy 68\n` +
      `/locksell 70 | /unlocksell 70\n` +
      `/clearbuylocks | /clearselllocks | /clearlocks\n` +
      `/buy 68 | /buy 68 50000\n` +
      `/sell 70 | /sell 70 150000 | /sellqty 70 2000\n` +
      `/buynow 50000 | /sellnow 2000\n` +
      `/replacebuy 68 66 | /replacesell 70 72\n` +
      `/cancelbuy 68 | /cancelsell 70 | /cancelall\n` +
      `/panic | /resume`
    );
    return;
  }

  if (text === "/pairs") {
    const st = await getState(env);
    ensureStateShape(st);
    await sendMsg(env, chatId, `📚 PAIR STATE\n\n${formatPairStateDetail(st)}`);
    return;
  }

  if (text === "/locks") {
    const st = await getState(env);
    ensureStateShape(st);
    await sendMsg(
      env,
      chatId,
      `🔒 LOCK STATE\n\n` +
      `BUY  : ${formatBuyLocks(st)}\n` +
      `SELL : ${formatSellLocks(st)}`
    );
    return;
  }

  if (text === "/orders") {
    await handleOrders(env, chatId);
    return;
  }

  if (text === "/ordersraw") {
    await handleOrdersRaw(env, chatId);
    return;
  }

  if (text === "/autogrid on") {
    const st = await getState(env);
    st.autoGridEnabled = true;
    await setState(env, st);
    await sendMsg(env, chatId, "✅ autoGrid ON");
    return;
  }

  if (text === "/autogrid off") {
    const st = await getState(env);
    st.autoGridEnabled = false;
    await setState(env, st);
    await sendMsg(env, chatId, "⛔ autoGrid OFF");
    return;
  }

  if (text.startsWith("/assume ")) {
    const n = Number(text0.split(/\s+/)[1]);
    if (!Number.isFinite(n) || n <= 0) {
      await sendMsg(env, chatId, "Format: /assume 68");
      return;
    }

    const st = await getState(env);
    st.assumedEntryPrice = Math.round(n);
    seedInitialPairStates(st);
    await setState(env, st);

    await sendMsg(env, chatId, `✅ assumed entry set to ${st.assumedEntryPrice}`);
    return;
  }

  if (text.startsWith("/seedbuy ")) {
    const n = Math.round(Number(text0.split(/\s+/)[1]));
    if (!BUY_TO_SELL.has(n)) {
      await sendMsg(env, chatId, "Format: /seedbuy 68");
      return;
    }

    const st = await getState(env);
    ensureStateShape(st);
    st.pairSide[String(n)] = "sell";
    await setState(env, st);

    await sendMsg(env, chatId, `✅ pair ${n}->${BUY_TO_SELL.get(n)} diset aktif di sisi SELL`);
    return;
  }

  if (text.startsWith("/seedsell ")) {
    const s = Math.round(Number(text0.split(/\s+/)[1]));
    const b = SELL_TO_BUY.get(s);
    if (!b) {
      await sendMsg(env, chatId, "Format: /seedsell 70");
      return;
    }

    const st = await getState(env);
    ensureStateShape(st);
    st.pairSide[String(b)] = "buy";
    await setState(env, st);

    await sendMsg(env, chatId, `✅ pair ${b}->${s} diset aktif di sisi BUY`);
    return;
  }

  if (text.startsWith("/lockbuy ")) {
    const n = Math.round(Number(text0.split(/\s+/)[1]));
    if (!BUY_TO_SELL.has(n)) {
      await sendMsg(env, chatId, "Format: /lockbuy 68");
      return;
    }

    const st = await getState(env);
    ensureStateShape(st);

    st.lockedBuys[String(n)] = { source: "manual", ts: nowSec() };
    st.pairSide[String(n)] = "sell";

    await setState(env, st);

    await sendMsg(
      env,
      chatId,
      `🔒 BUY ${n} locked manual\n` +
      `Pair ${n}->${BUY_TO_SELL.get(n)} dipaksa aktif di sisi SELL`
    );
    return;
  }

  if (text.startsWith("/unlockbuy ")) {
    const n = Math.round(Number(text0.split(/\s+/)[1]));
    const st = await getState(env);
    ensureStateShape(st);

    if (!st.lockedBuys[String(n)]) {
      await sendMsg(env, chatId, `ℹ️ BUY ${n} tidak sedang locked.`);
      return;
    }

    delete st.lockedBuys[String(n)];
    await setState(env, st);

    await sendMsg(env, chatId, `✅ BUY ${n} unlock manual`);
    return;
  }

  if (text.startsWith("/locksell ")) {
    const s = Math.round(Number(text0.split(/\s+/)[1]));
    const b = SELL_TO_BUY.get(s);
    if (!b) {
      await sendMsg(env, chatId, "Format: /locksell 70");
      return;
    }

    const st = await getState(env);
    ensureStateShape(st);

    st.lockedSells[String(s)] = { source: "manual", ts: nowSec() };
    st.pairSide[String(b)] = "buy";

    await setState(env, st);

    await sendMsg(
      env,
      chatId,
      `🔒 SELL ${s} locked manual\n` +
      `Pair ${b}->${s} dipaksa aktif di sisi BUY`
    );
    return;
  }

  if (text.startsWith("/unlocksell ")) {
    const s = Math.round(Number(text0.split(/\s+/)[1]));
    const st = await getState(env);
    ensureStateShape(st);

    if (!st.lockedSells[String(s)]) {
      await sendMsg(env, chatId, `ℹ️ SELL ${s} tidak sedang locked.`);
      return;
    }

    delete st.lockedSells[String(s)];
    await setState(env, st);

    await sendMsg(env, chatId, `✅ SELL ${s} unlock manual`);
    return;
  }

  if (text === "/clearbuylocks") {
    const st = await getState(env);
    ensureStateShape(st);
    const count = Object.keys(st.lockedBuys || {}).length;
    st.lockedBuys = {};
    await setState(env, st);
    await sendMsg(env, chatId, `✅ clear buy locks: ${count}`);
    return;
  }

  if (text === "/clearselllocks") {
    const st = await getState(env);
    ensureStateShape(st);
    const count = Object.keys(st.lockedSells || {}).length;
    st.lockedSells = {};
    await setState(env, st);
    await sendMsg(env, chatId, `✅ clear sell locks: ${count}`);
    return;
  }

  if (text === "/clearlocks") {
    const st = await getState(env);
    ensureStateShape(st);
    const countB = Object.keys(st.lockedBuys || {}).length;
    const countS = Object.keys(st.lockedSells || {}).length;
    st.lockedBuys = {};
    st.lockedSells = {};
    await setState(env, st);
    await sendMsg(env, chatId, `✅ clear all locks: buy=${countB}, sell=${countS}`);
    return;
  }

  if (text === "/reset") {
    const old = await getState(env);
    const st = {
      lastTradeTs: 0,
      lastTradeSig: "",
      lastReportTs: 0,
      lastReportedPrice: 0,
      assumedEntryPrice: old.assumedEntryPrice || ASSUMED_ENTRY_PRICE_DEFAULT,
      manualStopped: old.manualStopped === true,
      autoGridEnabled: old.autoGridEnabled !== false,
      lastBias: "",
      pairSide: {},
      lockedBuys: old.lockedBuys || {},
      lockedSells: old.lockedSells || {},
      gridRangeLow: Number(old.gridRangeLow || DEFAULT_GRID_RANGE_LOW),
      gridRangeHigh: Number(old.gridRangeHigh || DEFAULT_GRID_RANGE_HIGH),
      activeRange: Number(old.activeRange || DEFAULT_ACTIVE_RANGE),
      microActiveRange: Number(old.microActiveRange || DEFAULT_MICRO_ACTIVE_RANGE),
      buyNearGap: Number(old.buyNearGap || DEFAULT_BUY_NEAR_GAP),
      sellNearGap: Number(old.sellNearGap || DEFAULT_SELL_NEAR_GAP),
    };
    seedInitialPairStates(st);
    await setState(env, st);

    await sendMsg(env, chatId, "✅ RESET done. Pair state diinisialisasi ulang dari assumed entry.");
    return;
  }

  if (text.startsWith("/buy ")) {
    await handleManualBuy(env, chatId, text0);
    return;
  }

  if (text.startsWith("/sellqty ")) {
    await handleManualSellQty(env, chatId, text0);
    return;
  }

  if (text.startsWith("/sell ")) {
    await handleManualSell(env, chatId, text0);
    return;
  }

  if (text.startsWith("/buynow ")) {
    await handleBuyNow(env, chatId, text0);
    return;
  }

  if (text.startsWith("/sellnow ")) {
    await handleSellNow(env, chatId, text0);
    return;
  }

  if (text.startsWith("/replacebuy ")) {
    await handleReplaceBuy(env, chatId, text0);
    return;
  }

  if (text.startsWith("/replacesell ")) {
    await handleReplaceSell(env, chatId, text0);
    return;
  }

  if (text.startsWith("/cancelbuy ")) {
    await handleCancelBuyByPrice(env, chatId, text0);
    return;
  }

  if (text.startsWith("/cancelsell ")) {
    await handleCancelSellByPrice(env, chatId, text0);
    return;
  }

  if (text === "/cancelall") {
    await handleCancelAll(env, chatId);
    return;
  }

  if (text === "/pause") {
    await handlePause(env, chatId);
    return;
  }

  if (text === "/panic") {
    await handlePanic(env, chatId);
    return;
  }

  if (text === "/resume") {
    await handleResume(env, chatId);
    return;
  }

  await sendMsg(
    env,
    chatId,
    "Ketik:\n" +
    "/grid | /status | /range | /pairs | /locks | /orders | /ordersraw | /reset\n" +
    "/setrange 40 100\n" +
    "/autogrid on | /autogrid off\n" +
    "/assume 68 | /seedbuy 68 | /seedsell 70\n" +
    "/lockbuy 68 | /unlockbuy 68\n" +
    "/locksell 70 | /unlocksell 70\n" +
    "/clearbuylocks | /clearselllocks | /clearlocks\n" +
    "/buy 68 | /buy 68 50000\n" +
    "/sell 70 | /sell 70 150000 | /sellqty 70 2000\n" +
    "/buynow 50000 | /sellnow 2000\n" +
    "/replacebuy 68 66 | /replacesell 70 72\n" +
    "/cancelbuy 68 | /cancelsell 70 | /cancelall\n" +
    "/pause | /panic | /resume"
  );
}

// ================= COMMAND IMPLEMENTATION =================
async function handleOrders(env, chatId) {
  const nonceCtx = await createNonceCtx(env);
  try {
    const oo = await api.openOrders(env, GRID_PAIR_TAPI, nonceCtx);
    const { buyList, sellList } = extractOrdersAnyShape(oo);

    const buys = summarizeOrdersByPrice(buyList, "buy");
    const sells = summarizeOrdersByPrice(sellList, "sell");

    await sendMsg(
      env,
      chatId,
      `📦 OPEN ORDERS\n\n` +
      `BUY  : ${buys.join(", ") || "-"}\n` +
      `SELL : ${sells.join(", ") || "-"}\n` +
      `Count: buy=${buyList.length}, sell=${sellList.length}`
    );
  } catch (e) {
    await sendMsg(env, chatId, `❌ gagal baca open orders\n${String(e?.message || e)}`);
  } finally {
    await nonceCtx.flush();
  }
}

async function handleOrdersRaw(env, chatId) {
  const nonceCtx = await createNonceCtx(env);
  try {
    const oo = await api.openOrders(env, GRID_PAIR_TAPI, nonceCtx);
    const raw = JSON.stringify(oo, null, 2);

    const chunks = splitText(raw, 3500);
    for (let i = 0; i < chunks.length; i++) {
      await sendMsg(
        env,
        chatId,
        `🧪 ORDERS RAW ${i + 1}/${chunks.length}\n\n${chunks[i]}`
      );
    }
  } catch (e) {
    await sendMsg(env, chatId, `❌ gagal baca orders raw\n${String(e?.message || e)}`);
  } finally {
    await nonceCtx.flush();
  }
}

async function handleManualBuy(env, chatId, text0) {
  const envEnabled = String(env.TRADING_ENABLED || "").toLowerCase() === "true";
  const st = await getState(env);
  ensureStateShape(st);

  if (st.manualStopped || !envEnabled) {
    await sendMsg(env, chatId, "⛔ trading tidak aktif. Cek /status");
    return;
  }

  const parts = text0.split(/\s+/);
  const price = Math.round(Number(parts[1]));
  const amountIdrRaw = parts[2] != null ? Number(parts[2]) : dynamicBuyAmount(price, "BALANCE");

  if (!BUY_TO_SELL.has(price)) {
    await sendMsg(env, chatId, "Format: /buy 68 atau /buy 68 50000");
    return;
  }

  if (!Number.isFinite(amountIdrRaw) || amountIdrRaw <= 0) {
    await sendMsg(env, chatId, "Nominal IDR tidak valid");
    return;
  }

  if (isBuyLocked(st, price)) {
    await sendMsg(env, chatId, `⛔ BUY ${price} sedang locked.`);
    return;
  }

  const amountIdr = roundTo(amountIdrRaw, IDR_ROUND_TO);
  const nonceCtx = await createNonceCtx(env);

  try {
    const oo = await api.openOrders(env, GRID_PAIR_TAPI, nonceCtx);
    if (hasOrderAt(oo, "buy", price)) {
      await sendMsg(env, chatId, `ℹ️ BUY ${price} sudah ada di open orders.`);
      return;
    }

    const info = await api.getInfo(env, nonceCtx);
    const idrFree = Number(info.balance?.idr || 0);

    if (idrFree < amountIdr) {
      await sendMsg(
        env,
        chatId,
        `⛔ saldo IDR free tidak cukup\nNeed : ${amountIdr.toLocaleString("id-ID")}\nFree : ${Math.round(idrFree).toLocaleString("id-ID")}`
      );
      return;
    }

    await api.trade(env, GRID_PAIR_TAPI, "buy", price, { idr: String(amountIdr) }, nonceCtx);

    await sendMsg(
      env,
      chatId,
      `✅ BUY placed\n` +
      `Price      : ${price}\n` +
      `Nominal IDR: ${amountIdr.toLocaleString("id-ID")}`
    );
  } catch (e) {
    await sendMsg(env, chatId, `❌ gagal place BUY ${price}\n${String(e?.message || e)}`);
  } finally {
    await nonceCtx.flush();
  }
}

async function handleManualSell(env, chatId, text0) {
  const envEnabled = String(env.TRADING_ENABLED || "").toLowerCase() === "true";
  const st = await getState(env);
  ensureStateShape(st);

  if (st.manualStopped || !envEnabled) {
    await sendMsg(env, chatId, "⛔ trading tidak aktif. Cek /status");
    return;
  }

  const parts = text0.split(/\s+/);
  const price = Math.round(Number(parts[1]));
  const targetIdrRaw = parts[2] != null ? Number(parts[2]) : dynamicSellTarget(price, "BALANCE");

  if (!SELL_TO_BUY.has(price)) {
    await sendMsg(env, chatId, "Format: /sell 70 atau /sell 70 150000");
    return;
  }

  if (!Number.isFinite(targetIdrRaw) || targetIdrRaw <= 0) {
    await sendMsg(env, chatId, "Target IDR tidak valid");
    return;
  }

  if (isSellLocked(st, price)) {
    await sendMsg(env, chatId, `⛔ SELL ${price} sedang locked.`);
    return;
  }

  const qty = calcSellQty(price, targetIdrRaw);
  if (qty <= 0) {
    await sendMsg(env, chatId, `❌ qty SELL invalid untuk price ${price}`);
    return;
  }

  const nonceCtx = await createNonceCtx(env);

  try {
    const oo = await api.openOrders(env, GRID_PAIR_TAPI, nonceCtx);
    if (hasOrderAt(oo, "sell", price)) {
      await sendMsg(env, chatId, `ℹ️ SELL ${price} sudah ada di open orders.`);
      return;
    }

    const info = await api.getInfo(env, nonceCtx);
    const dgbFree = Number(info.balance?.dgb || 0);

    if (dgbFree < qty) {
      await sendMsg(
        env,
        chatId,
        `⛔ saldo DGB free tidak cukup\nNeed : ${fmtQty(qty)}\nFree : ${fmtQty(dgbFree)}`
      );
      return;
    }

    await api.trade(env, GRID_PAIR_TAPI, "sell", price, { dgb: String(qty) }, nonceCtx);

    await sendMsg(
      env,
      chatId,
      `✅ SELL placed\n` +
      `Price      : ${price}\n` +
      `Qty DGB    : ${fmtQty(qty)}\n` +
      `Target IDR : ${Math.round(targetIdrRaw).toLocaleString("id-ID")}`
    );
  } catch (e) {
    await sendMsg(env, chatId, `❌ gagal place SELL ${price}\n${String(e?.message || e)}`);
  } finally {
    await nonceCtx.flush();
  }
}

async function handleManualSellQty(env, chatId, text0) {
  const envEnabled = String(env.TRADING_ENABLED || "").toLowerCase() === "true";
  const st = await getState(env);
  ensureStateShape(st);

  if (st.manualStopped || !envEnabled) {
    await sendMsg(env, chatId, "⛔ trading tidak aktif. Cek /status");
    return;
  }

  const parts = text0.split(/\s+/);
  const price = Math.round(Number(parts[1]));
  const qty = Number(parts[2]);

  if (!SELL_TO_BUY.has(price)) {
    await sendMsg(env, chatId, "Format: /sellqty 70 2000");
    return;
  }

  if (!Number.isFinite(qty) || qty <= 0) {
    await sendMsg(env, chatId, "Qty DGB tidak valid");
    return;
  }

  if (isSellLocked(st, price)) {
    await sendMsg(env, chatId, `⛔ SELL ${price} sedang locked.`);
    return;
  }

  const nonceCtx = await createNonceCtx(env);

  try {
    const oo = await api.openOrders(env, GRID_PAIR_TAPI, nonceCtx);
    if (hasOrderAt(oo, "sell", price)) {
      await sendMsg(env, chatId, `ℹ️ SELL ${price} sudah ada di open orders.`);
      return;
    }

    const info = await api.getInfo(env, nonceCtx);
    const dgbFree = Number(info.balance?.dgb || 0);

    if (dgbFree < qty) {
      await sendMsg(
        env,
        chatId,
        `⛔ saldo DGB free tidak cukup\nNeed : ${fmtQty(qty)}\nFree : ${fmtQty(dgbFree)}`
      );
      return;
    }

    await api.trade(env, GRID_PAIR_TAPI, "sell", price, { dgb: String(qty) }, nonceCtx);

    await sendMsg(
      env,
      chatId,
      `✅ SELL QTY placed\n` +
      `Price   : ${price}\n` +
      `Qty DGB : ${fmtQty(qty)}`
    );
  } catch (e) {
    await sendMsg(env, chatId, `❌ gagal place SELL QTY ${price}\n${String(e?.message || e)}`);
  } finally {
    await nonceCtx.flush();
  }
}

async function handleBuyNow(env, chatId, text0) {
  const parts = text0.split(/\s+/);
  const idr = Number(parts[1]);

  if (!Number.isFinite(idr) || idr <= 0) {
    await sendMsg(env, chatId, "Format: /buynow 50000");
    return;
  }

  const trades = await getTrades(GRID_PAIR_PUBLIC);
  const last = trades ? Number(trades[0]?.price || 0) : 0;
  if (!last) {
    await sendMsg(env, chatId, "❌ gagal ambil last price");
    return;
  }

  const price = findNearestAllowedBuy(Math.round(last));
  if (!price) {
    await sendMsg(env, chatId, `❌ tidak ada buy level pair yang cocok dekat harga ${Math.round(last)}`);
    return;
  }

  await handleManualBuy(env, chatId, `/buy ${price} ${Math.round(idr)}`);
}

async function handleSellNow(env, chatId, text0) {
  const parts = text0.split(/\s+/);
  const qty = Number(parts[1]);

  if (!Number.isFinite(qty) || qty <= 0) {
    await sendMsg(env, chatId, "Format: /sellnow 2000");
    return;
  }

  const trades = await getTrades(GRID_PAIR_PUBLIC);
  const last = trades ? Number(trades[0]?.price || 0) : 0;
  if (!last) {
    await sendMsg(env, chatId, "❌ gagal ambil last price");
    return;
  }

  const price = findNearestAllowedSell(Math.round(last));
  if (!price) {
    await sendMsg(env, chatId, `❌ tidak ada sell level pair yang cocok dekat harga ${Math.round(last)}`);
    return;
  }

  await handleManualSellQty(env, chatId, `/sellqty ${price} ${qty}`);
}

async function handleReplaceBuy(env, chatId, text0) {
  const parts = text0.split(/\s+/);
  const oldPrice = Math.round(Number(parts[1]));
  const newPrice = Math.round(Number(parts[2]));

  if (!BUY_TO_SELL.has(oldPrice) || !BUY_TO_SELL.has(newPrice)) {
    await sendMsg(env, chatId, "Format: /replacebuy 68 66");
    return;
  }

  await handleCancelBuyByPrice(env, chatId, `/cancelbuy ${oldPrice}`);
  await handleManualBuy(env, chatId, `/buy ${newPrice} ${dynamicBuyAmount(newPrice, "BALANCE")}`);
}

async function handleReplaceSell(env, chatId, text0) {
  const parts = text0.split(/\s+/);
  const oldPrice = Math.round(Number(parts[1]));
  const newPrice = Math.round(Number(parts[2]));

  if (!SELL_TO_BUY.has(oldPrice) || !SELL_TO_BUY.has(newPrice)) {
    await sendMsg(env, chatId, "Format: /replacesell 70 72");
    return;
  }

  await handleCancelSellByPrice(env, chatId, `/cancelsell ${oldPrice}`);
  await handleManualSell(env, chatId, `/sell ${newPrice} ${dynamicSellTarget(newPrice, "BALANCE")}`);
}

async function handleCancelBuyByPrice(env, chatId, text0) {
  const n = Math.round(Number(text0.split(/\s+/)[1]));
  if (!Number.isFinite(n) || n <= 0) {
    await sendMsg(env, chatId, "Format: /cancelbuy 68");
    return;
  }

  const nonceCtx = await createNonceCtx(env);
  try {
    const oo = await api.openOrders(env, GRID_PAIR_TAPI, nonceCtx);
    const { buyList } = extractOrdersAnyShape(oo);

    const targets = buyList.filter(o => orderPrice(o) === n);
    if (!targets.length) {
      await sendMsg(env, chatId, `ℹ️ tidak ada BUY open di price ${n}`);
      return;
    }

    let ok = 0, fail = 0;
    for (const o of targets) {
      try {
        await api.cancelOrder(env, GRID_PAIR_TAPI, orderId(o), "buy", nonceCtx);
        ok++;
      } catch {
        fail++;
      }
    }

    await sendMsg(
      env,
      chatId,
      `✅ cancel BUY ${n}\n` +
      `found=${targets.length}, ok=${ok}, fail=${fail}`
    );
  } catch (e) {
    await sendMsg(env, chatId, `❌ gagal cancel BUY ${n}\n${String(e?.message || e)}`);
  } finally {
    await nonceCtx.flush();
  }
}

async function handleCancelSellByPrice(env, chatId, text0) {
  const n = Math.round(Number(text0.split(/\s+/)[1]));
  if (!Number.isFinite(n) || n <= 0) {
    await sendMsg(env, chatId, "Format: /cancelsell 70");
    return;
  }

  const nonceCtx = await createNonceCtx(env);
  try {
    const oo = await api.openOrders(env, GRID_PAIR_TAPI, nonceCtx);
    const { sellList } = extractOrdersAnyShape(oo);

    const targets = sellList.filter(o => orderPrice(o) === n);
    if (!targets.length) {
      await sendMsg(env, chatId, `ℹ️ tidak ada SELL open di price ${n}`);
      return;
    }

    let ok = 0, fail = 0;
    for (const o of targets) {
      try {
        await api.cancelOrder(env, GRID_PAIR_TAPI, orderId(o), "sell", nonceCtx);
        ok++;
      } catch {
        fail++;
      }
    }

    await sendMsg(
      env,
      chatId,
      `✅ cancel SELL ${n}\n` +
      `found=${targets.length}, ok=${ok}, fail=${fail}`
    );
  } catch (e) {
    await sendMsg(env, chatId, `❌ gagal cancel SELL ${n}\n${String(e?.message || e)}`);
  } finally {
    await nonceCtx.flush();
  }
}

async function handleCancelAll(env, chatId) {
  const nonceCtx = await createNonceCtx(env);
  try {
    const oo = await api.openOrders(env, GRID_PAIR_TAPI, nonceCtx);
    const { buyList, sellList } = extractOrdersAnyShape(oo);

    let ok = 0, fail = 0;

    for (const o of buyList) {
      try {
        await api.cancelOrder(env, GRID_PAIR_TAPI, orderId(o), "buy", nonceCtx);
        ok++;
      } catch {
        fail++;
      }
    }

    for (const o of sellList) {
      try {
        await api.cancelOrder(env, GRID_PAIR_TAPI, orderId(o), "sell", nonceCtx);
        ok++;
      } catch {
        fail++;
      }
    }

    await sendMsg(
      env,
      chatId,
      `✅ CANCEL ALL\n` +
      `buy=${buyList.length}, sell=${sellList.length}\n` +
      `ok=${ok}, fail=${fail}`
    );
  } catch (e) {
    await sendMsg(env, chatId, `❌ gagal cancel all\n${String(e?.message || e)}`);
  } finally {
    await nonceCtx.flush();
  }
}

async function handlePanic(env, chatId) {
  const nonceCtx = await createNonceCtx(env);
  try {
    const st = await getState(env);
    st.manualStopped = true;
    st.manualPaused = false;
    await setState(env, st);

    let cancelOk = 0;
    let cancelFail = 0;

    try {
      const oo = await api.openOrders(env, GRID_PAIR_TAPI, nonceCtx);
      const { buyList, sellList } = extractOrdersAnyShape(oo);

      for (const o of buyList) {
        try {
          await api.cancelOrder(env, GRID_PAIR_TAPI, orderId(o), "buy", nonceCtx);
          cancelOk++;
        } catch {
          cancelFail++;
        }
      }

      for (const o of sellList) {
        try {
          await api.cancelOrder(env, GRID_PAIR_TAPI, orderId(o), "sell", nonceCtx);
          cancelOk++;
        } catch {
          cancelFail++;
        }
      }
    } catch {}

    await sendMsg(
      env,
      chatId,
      `🚨 PANIC MODE ACTIVATED\n\n` +
      `Trading manual stop : ON\n` +
      `Cancel all orders   : ok=${cancelOk}, fail=${cancelFail}\n` +
      `Bot status: STOPPED ⛔`
    );
  } finally {
    await nonceCtx.flush();
  }
}

async function handlePause(env, chatId) {
  const st = await getState(env);
  st.manualPaused = true;
  await setState(env, st);

  await sendMsg(
    env,
    chatId,
    `⏸️ PAUSE requested\n\n` +
    `manualPaused  : YES\n` +
    `manualStopped : ${st.manualStopped ? "YES" : "NO"}\n` +
    `tradingActive : NO ⛔`
  );
}

async function handleResume(env, chatId) {
  const st = await getState(env);
  st.manualStopped = false;
  st.manualPaused = false;
  await setState(env, st);

  const envEnabled = String(env.TRADING_ENABLED || "").toLowerCase() === "true";
  await sendMsg(
    env,
    chatId,
    `✅ RESUME requested\n\n` +
    `manualPaused  : NO\n` +
    `manualStopped : NO\n` +
    `envEnabled    : ${envEnabled ? "true" : "false"}\n` +
    `tradingActive : ${envEnabled ? "YES ✅" : "NO ⛔"}`
  );
}

// ================= RUN =================
async function runOnce(env, chatId, forceMsg) {
  const gotLock = await acquireRunLock(env);
  if (!gotLock && !forceMsg) return;

  const nonceCtx = await createNonceCtx(env);

  try {
    const envEnabled = String(env.TRADING_ENABLED || "").toLowerCase() === "true";

    const trades = await getTrades(GRID_PAIR_PUBLIC);
    const last = trades ? Number(trades[0]?.price || 0) : 0;
    if (!last) {
      if (forceMsg) await sendMsg(env, chatId, "❌ gagal ambil last price");
      return;
    }
    const lastR = Math.round(last);
    const marketState = analyzeMarketState(trades || [], lastR);
    const marketAnalysis = analyzeTrendAndProjection(trades || [], lastR);

    const st = await getState(env);
    ensureStateShape(st);

    if (syncRangeToPrice(st, lastR)) {
      await setState(env, st);
    }

    const cfg = getRuntimeConfig(st);

    if (!Object.keys(st.pairSide).length) {
      seedInitialPairStates(st);
      await setState(env, st);
    }

    const tradingActive = envEnabled && !st.manualStopped && !st.manualPaused;

    if (!st.lastTradeTs) {
      const mk = await getNewestTradeMark(env, nonceCtx);
      st.lastTradeTs = mk.ts;
      st.lastTradeSig = mk.sig;
      await setState(env, st);
    }

    let idrFree = 0, dgbFree = 0, idrHold = 0, dgbHold = 0;
    let idrTotal = 0, dgbTotal = 0, equity = 0;

    try {
      const info = await api.getInfo(env, nonceCtx);
      idrFree  = Number(info.balance?.idr || 0);
      dgbFree  = Number(info.balance?.dgb || 0);
      idrHold  = Number(info.balance_hold?.idr || 0);
      dgbHold  = Number(info.balance_hold?.dgb || 0);
      idrTotal = idrFree + idrHold;
      dgbTotal = dgbFree + dgbHold;
      equity = idrTotal + (dgbTotal * last);
    } catch {}

    let oo = await api.openOrders(env, GRID_PAIR_TAPI, nonceCtx);

    const tradesNew = await readNewTradesRaw(env, nonceCtx, st.lastTradeTs, st.lastTradeSig);

    oo = await api.openOrders(env, GRID_PAIR_TAPI, nonceCtx);

    const filledEvents = [];
    for (const t of tradesNew.items) {
      const stillOpen = hasOrderAt(oo, t.side, t.price);
      if (!stillOpen) filledEvents.push(t);
    }

    st.lastTradeTs = tradesNew.newestTs;
    st.lastTradeSig = tradesNew.newestSig;

    for (const e of filledEvents.sort((a,b)=>a.ts-b.ts || a.sig.localeCompare(b.sig))) {
      if (e.side === "buy") {
        const sellP = BUY_TO_SELL.get(e.price);
        if (sellP) st.pairSide[String(e.price)] = "sell";
      } else {
        const buyP = SELL_TO_BUY.get(e.price);
        if (buyP) st.pairSide[String(buyP)] = "buy";
      }
    }

    const autoLevels = buildAutoInventoryLevels(cfg.gridRangeLow, cfg.gridRangeHigh);
    const coinRatio = getInventoryRatio(idrTotal, dgbTotal, last);
    const targetCoinRatio = getTargetCoinRatio(lastR, autoLevels);
    const bias = getInventoryBias(coinRatio, targetCoinRatio);
    const targetInfo = getAutoTargetInfo(lastR, autoLevels);

    let cancelResult = { cancelOk: 0, cancelFail: 0 };
    let rebalanceResult = {
      buyReplaced: 0,
      sellReplaced: 0,
      biasChanged: false
    };
    let placeResult = { buyPlaced: 0, sellPlaced: 0, buySkipped: 0, sellSkipped: 0, buySkippedInventory: 0, sellSkippedInventory: 0, buySkippedVolume: 0, sellSkippedVolume: 0 };

    if (tradingActive && st.autoGridEnabled) {
      cancelResult = await autoCancelFarOrders(env, st, lastR, nonceCtx, marketState, cfg);

      oo = await api.openOrders(env, GRID_PAIR_TAPI, nonceCtx);
      let infoAfterCancel = null;
      try {
        infoAfterCancel = await api.getInfo(env, nonceCtx);
      } catch {}

      rebalanceResult = await rebalanceOpenOrders(
        env,
        st,
        oo,
        lastR,
        bias,
        nonceCtx,
        infoAfterCancel,
        marketState
      );

      oo = await api.openOrders(env, GRID_PAIR_TAPI, nonceCtx);

      try {
        const info2 = await api.getInfo(env, nonceCtx);
        idrFree  = Number(info2.balance?.idr || 0);
        dgbFree  = Number(info2.balance?.dgb || 0);
        idrHold  = Number(info2.balance_hold?.idr || 0);
        dgbHold  = Number(info2.balance_hold?.dgb || 0);
        idrTotal = idrFree + idrHold;
        dgbTotal = dgbFree + dgbHold;
        equity = idrTotal + (dgbTotal * last);
      } catch {}

      placeResult = await autoGenerateOrders(env, st, lastR, idrFree, dgbFree, bias, nonceCtx, marketState, cfg, coinRatio, targetCoinRatio);
    }

    const prevBiasForReport = st.lastBias || "-";
    st.lastBias = bias;

    if (filledEvents.length || tradesNew.items.length || rebalanceResult.biasChanged) {
      await setState(env, st);
    }

    oo = await api.openOrders(env, GRID_PAIR_TAPI, nonceCtx);
    const { buyList, sellList } = extractOrdersAnyShape(oo);

    const reportBuyLevels = summarizeOrdersByPrice(buyList, "buy");
    const reportSellLevels = summarizeOrdersByPrice(sellList, "sell");

    try {
      const infoFinal = await api.getInfo(env, nonceCtx);
      idrFree  = Number(infoFinal.balance?.idr || 0);
      dgbFree  = Number(infoFinal.balance?.dgb || 0);
      idrHold  = Number(infoFinal.balance_hold?.idr || 0);
      dgbHold  = Number(infoFinal.balance_hold?.dgb || 0);
      idrTotal = idrFree + idrHold;
      dgbTotal = dgbFree + dgbHold;
      equity = idrTotal + (dgbTotal * last);
    } catch {}

    const now = nowSec();
    const priceChanged = await shouldSendPriceUpdate(env, lastR);
    const heartbeatDue = (now - Number(st.lastReportTs || 0)) >= HEARTBEAT_INTERVAL_SEC;

    const shouldSend =
      forceMsg ||
      priceChanged ||
      heartbeatDue;

    if (shouldSend) {
      st.lastReportTs = now;
      st.lastReportedPrice = lastR;
      await setState(env, st);

      const msg =
        `🧠 FULL MANUAL + AUTO GRID DGBIDR V3.5\n\n` +
        `Time   : ${fmtWIBTime()}\n` +
        `Last   : ${lastR}\n` +
        `Regime : ${marketState.regime}\n` +
        `Trend  : ${marketAnalysis.trend}\n` +
        `Projection : ${marketAnalysis.projection}\n` +
        `Mode   : FULL MANUAL + AUTO INVENTORY\n` +
        `Assume : ${Math.round(st.assumedEntryPrice || ASSUMED_ENTRY_PRICE_DEFAULT)}\n` +
        `Auto   : ${st.autoGridEnabled ? "ON ✅" : "OFF ⛔"}\n` +
        `Paused : ${st.manualPaused ? "YES ⏸️" : "NO"}\n\n` +
        `Range:\n` +
        `• Grid range         : ${cfg.gridRangeLow}-${cfg.gridRangeHigh}\n` +
        `• Active range       : ±${cfg.activeRange}\n` +
        `• Micro active range : ±${cfg.microActiveRange}\n\n` +
        `Inventory:\n` +
        `• Coin ratio current : ${(coinRatio * 100).toFixed(1)}%\n` +
        `• Coin ratio target  : ${(targetCoinRatio * 100).toFixed(1)}%\n` +
        `• Auto level floor   : ${targetInfo.floorPrice}\n` +
        `• Auto level ceil    : ${targetInfo.ceilPrice}\n` +
        `• Bias               : ${bias}\n` +
        `• Prev bias          : ${prevBiasForReport}\n` +
        `• Sideways range     : ${marketState.minPrice ?? "-"}-${marketState.maxPrice ?? "-"}\n` +
        `• Sideways range pct : ${Number.isFinite(marketState.rangePct) ? (marketState.rangePct * 100).toFixed(2) + "%" : "-"}\n\n` +
        `Locked BUY  : ${formatBuyLocks(st)}\n` +
        `Locked SELL : ${formatSellLocks(st)}\n` +
        `Open BUY    : ${reportBuyLevels.join(", ") || "-"}\n` +
        `Open SELL   : ${reportSellLevels.join(", ") || "-"}\n\n` +
        `New fills completed: ${filledEvents.length}\n` +
        `Fill detail : ${filledEvents.map(x => `${x.side}@${x.price}`).join(", ") || "-"}\n\n` +
        `Auto Cancel : ok=${cancelResult.cancelOk}, fail=${cancelResult.cancelFail}\n` +
        `Rebalance   : biasChanged=${rebalanceResult.biasChanged ? "YES" : "NO"}, buy=${rebalanceResult.buyReplaced}, sell=${rebalanceResult.sellReplaced}\n` +
        `Auto Place  : buy=${placeResult.buyPlaced}, sell=${placeResult.sellPlaced}, skippedBuy=${placeResult.buySkipped}, skippedSell=${placeResult.sellSkipped}\n` +
        `Inventory Skip: buy=${placeResult.buySkippedInventory}, sell=${placeResult.sellSkippedInventory}\n` +
        `Volume Skip    : buy=${placeResult.buySkippedVolume}, sell=${placeResult.sellSkippedVolume}\n` +
        `• Keterangan : pembelian/sale dapat di-skip saat inventory sudah di atas/bawah target atau volume rendah\n\n` +
        `manualStopped  : ${st.manualStopped ? "YES ⛔" : "NO"}\n` +
        `TRADING_ENABLED(env): ${envEnabled ? "true" : "false"}\n` +
        `Trading Active : ${tradingActive ? "YES ✅" : "NO ⛔"}\n\n` +
        `📌 Estimasi Aset (IDR)\n` +
        `• Equity est : ${Math.round(equity).toLocaleString("id-ID")} IDR\n` +
        `• IDR total/free/hold : ${Math.round(idrTotal).toLocaleString("id-ID")} / ${Math.round(idrFree).toLocaleString("id-ID")} / ${Math.round(idrHold).toLocaleString("id-ID")}\n` +
        `• DGB total/free/hold : ${fmtQty(dgbTotal)} / ${fmtQty(dgbFree)} / ${fmtQty(dgbHold)}\n`;

      await sendMsg(env, chatId, msg);
    }

  } finally {
    await nonceCtx.flush();
  }
}

// ================= AUTO GRID ENGINE =================
function getRuntimeConfig(st) {
  const low = Math.round(Number(st.gridRangeLow || DEFAULT_GRID_RANGE_LOW));
  const high = Math.round(Number(st.gridRangeHigh || DEFAULT_GRID_RANGE_HIGH));

  return {
    gridRangeLow: low,
    gridRangeHigh: high,
    activeRange: Math.max(1, Math.round(Number(st.activeRange || DEFAULT_ACTIVE_RANGE))),
    microActiveRange: Math.max(1, Math.round(Number(st.microActiveRange || DEFAULT_MICRO_ACTIVE_RANGE))),
    buyNearGap: Math.max(0, Math.round(Number(st.buyNearGap || DEFAULT_BUY_NEAR_GAP))),
    sellNearGap: Math.max(0, Math.round(Number(st.sellNearGap || DEFAULT_SELL_NEAR_GAP))),
  };
}

function syncRangeToPrice(st, last) {
  const low = Math.round(Number(st.gridRangeLow || DEFAULT_GRID_RANGE_LOW));
  const high = Math.round(Number(st.gridRangeHigh || DEFAULT_GRID_RANGE_HIGH));
  if (low >= high) return false;

  const width = Math.max(1, high - low);
  const margin = Math.max(1, Math.round(width * 0.25));

  if (last >= low + margin && last <= high - margin) return false;

  let newLow = Math.round(last - Math.floor(width / 2));
  let newHigh = newLow + width;

  if (newLow < PAIR_BUY_MIN) {
    newLow = PAIR_BUY_MIN;
    newHigh = newLow + width;
  }
  if (newHigh > PAIR_BUY_MAX) {
    newHigh = PAIR_BUY_MAX;
    newLow = Math.max(PAIR_BUY_MIN, newHigh - width);
  }

  if (newLow === low && newHigh === high) return false;

  st.gridRangeLow = newLow;
  st.gridRangeHigh = newHigh;
  updatePairMaps(newLow, newHigh); // Update pair maps for new range
  return true;
}

function getInventoryRatio(idr, dgb, price) {
  const total = Number(idr || 0) + (Number(dgb || 0) * Number(price || 0));
  if (total <= 0) return 0;
  const coinValue = Number(dgb || 0) * Number(price || 0);
  return coinValue / total;
}

function buildAutoInventoryLevels(rangeLow, rangeHigh) {
  const buyLevels = [...new Set(
    PAIRS
      .map(([buy]) => Number(buy))
      .filter(p => Number.isFinite(p) && p >= rangeLow && p <= rangeHigh)
  )].sort((a, b) => a - b);

  if (!buyLevels.length) {
    return [{ price: rangeLow, ratio: AUTO_TARGET_MAX_RATIO }];
  }

  if (buyLevels.length === 1) {
    return [{ price: buyLevels[0], ratio: AUTO_TARGET_MAX_RATIO }];
  }

  const out = [];
  const steps = buyLevels.length - 1;
  const diff = AUTO_TARGET_MAX_RATIO - AUTO_TARGET_MIN_RATIO;

  for (let i = 0; i < buyLevels.length; i++) {
    const t = i / steps;
    const ratio = AUTO_TARGET_MAX_RATIO - (diff * t);
    out.push({
      price: buyLevels[i],
      ratio: clampRatio(ratio)
    });
  }

  return out;
}

function clampRatio(x) {
  const n = Number(x || 0);
  if (!Number.isFinite(n)) return 0.5;
  return Math.max(0.01, Math.min(0.99, n));
}

function getAutoTargetInfo(price, levels) {
  const p = Number(price || 0);

  let floor = levels[0];
  let ceil = levels[levels.length - 1];

  for (const row of levels) {
    if (row.price <= p) floor = row;
    if (row.price >= p) {
      ceil = row;
      break;
    }
  }

  return {
    floorPrice: floor?.price ?? levels[0].price,
    ceilPrice: ceil?.price ?? levels[levels.length - 1].price,
    floorRatio: floor?.ratio ?? levels[0].ratio,
    ceilRatio: ceil?.ratio ?? levels[levels.length - 1].ratio
  };
}

function getTargetCoinRatio(price, levels) {
  const p = Number(price || 0);

  if (!levels.length) return 0.5;
  if (p <= levels[0].price) return levels[0].ratio;
  if (p >= levels[levels.length - 1].price) return levels[levels.length - 1].ratio;

  for (let i = 0; i < levels.length - 1; i++) {
    const a = levels[i];
    const b = levels[i + 1];

    if (p === a.price) return a.ratio;
    if (p === b.price) return b.ratio;

    if (p > a.price && p < b.price) {
      const span = b.price - a.price;
      const pos = (p - a.price) / span;
      return clampRatio(a.ratio + ((b.ratio - a.ratio) * pos));
    }
  }

  return levels[levels.length - 1].ratio;
}

function getInventoryBias(currentRatio, targetRatio) {
  if (currentRatio < targetRatio - INVENTORY_BIAS_TOLERANCE) return "BUY";
  if (currentRatio > targetRatio + INVENTORY_BIAS_TOLERANCE) return "SELL";
  return "BALANCE";
}

function dynamicBuyAmount(price, bias) {
  const p = Number(price || 0);
  let base = 100000;

  if (p <= 65) base = 320000;
  else if (p <= 70) base = 260000;
  else if (p <= 75) base = 180000;
  else if (p <= 80) base = 140000;
  else if (p <= 90) base = 120000;
  else base = 100000;

  if (bias === "BUY") base *= 1.25;
  if (bias === "SELL") base *= 0.65;

  return roundTo(base, IDR_ROUND_TO);
}

function dynamicSellTarget(price, bias) {
  const p = Number(price || 0);
  let base = 120000;

  if (p >= 98) base = 320000;
  else if (p >= 94) base = 280000;
  else if (p >= 90) base = 240000;
  else if (p >= 85) base = 200000;
  else if (p >= 80) base = 170000;
  else base = 120000;

  if (bias === "SELL") base *= 1.25;
  if (bias === "BUY") base *= 0.75;

  return roundTo(base, IDR_ROUND_TO);
}

function analyzeMarketState(trades, lastPrice) {
  const now = nowSec();

  const xs = (Array.isArray(trades) ? trades : [])
    .map(t => ({
      price: Math.round(Number(t?.price || 0)),
      ts: Number(t?.date || 0)
    }))
    .filter(x =>
      Number.isFinite(x.price) &&
      x.price > 0 &&
      Number.isFinite(x.ts) &&
      (now - x.ts) <= SIDEWAYS_LOOKBACK_SEC
    );

  if (xs.length < SIDEWAYS_MIN_TRADES) {
    return {
      regime: "NORMAL",
      isSideways: false,
      minPrice: lastPrice,
      maxPrice: lastPrice,
      centerPrice: lastPrice,
      rangePct: 0,
      lastVsCenter: 0
    };
  }

  let minPrice = Infinity;
  let maxPrice = -Infinity;

  for (const x of xs) {
    if (x.price < minPrice) minPrice = x.price;
    if (x.price > maxPrice) maxPrice = x.price;
  }

  const centerPrice = (minPrice + maxPrice) / 2;
  const span = Math.max(1, maxPrice - minPrice);
  const rangePct = centerPrice > 0 ? (span / centerPrice) : 0;
  const lastVsCenter = (Number(lastPrice || 0) - centerPrice) / span;

  const isSideways =
    rangePct <= SIDEWAYS_MAX_RANGE_PCT &&
    Math.abs(lastVsCenter) <= SIDEWAYS_CENTER_TOLERANCE;

  return {
    regime: isSideways ? "SIDEWAYS_MICRO" : "NORMAL",
    isSideways,
    minPrice,
    maxPrice,
    centerPrice,
    rangePct,
    lastVsCenter
  };
}

function analyzeTrendAndProjection(trades, lastPrice) {
  const now = nowSec();
  const recentTrades = (Array.isArray(trades) ? trades : [])
    .map(t => ({
      price: Number(t?.price || 0),
      ts: Number(t?.date || 0)
    }))
    .filter(x =>
      Number.isFinite(x.price) &&
      x.price > 0 &&
      Number.isFinite(x.ts) &&
      (now - x.ts) <= 3600 // 1 jam terakhir
    )
    .sort((a, b) => a.ts - b.ts);

  if (recentTrades.length < 10) {
    return {
      trend: "UNKNOWN",
      projection: "Data tidak cukup untuk analisis"
    };
  }

  // Hitung rata-rata harga di setengah pertama dan kedua
  const mid = Math.floor(recentTrades.length / 2);
  const firstHalf = recentTrades.slice(0, mid);
  const secondHalf = recentTrades.slice(mid);

  const avgFirst = firstHalf.reduce((sum, t) => sum + t.price, 0) / firstHalf.length;
  const avgSecond = secondHalf.reduce((sum, t) => sum + t.price, 0) / secondHalf.length;

  const changePct = ((avgSecond - avgFirst) / avgFirst) * 100;

  let trend = "NEUTRAL";
  if (changePct > 2) trend = "BULLISH";
  else if (changePct < -2) trend = "BEARISH";

  // Proyeksi sederhana berdasarkan momentum
  const momentum = changePct > 0 ? "naik" : changePct < 0 ? "turun" : "stabil";
  const projection = `Harga mungkin ${momentum} ${Math.abs(changePct).toFixed(1)}% dalam waktu dekat`;

  return {
    trend,
    projection,
    changePct: changePct.toFixed(2) + "%"
  };
}

function pairSpreadPctFromBuy(buyPrice) {
  const b = Number(buyPrice || 0);
  const s = Number(BUY_TO_SELL.get(b) || 0);
  if (!b || !s || s <= b) return 0;
  return (s - b) / b;
}

function pairSpreadPctFromSell(sellPrice) {
  const s = Number(sellPrice || 0);
  const b = Number(SELL_TO_BUY.get(s) || 0);
  if (!b || !s || s <= b) return 0;
  return (s - b) / b;
}

function dynamicBuyAmountAdaptive(price, bias, marketState) {
  let amt = dynamicBuyAmount(price, bias);
  if (marketState?.isSideways) amt = roundTo(amt * MICRO_SIZE_FACTOR, IDR_ROUND_TO);
  return amt;
}

function dynamicSellTargetAdaptive(price, bias, marketState) {
  let amt = dynamicSellTarget(price, bias);
  if (marketState?.isSideways) amt = roundTo(amt * MICRO_SIZE_FACTOR, IDR_ROUND_TO);
  return amt;
}

async function rebalanceOpenOrders(env, st, openOrders, last, bias, nonceCtx, infoAfterCancel = null, marketState = null) {
  if (!REBALANCE_ENABLED) {
    return { buyReplaced: 0, sellReplaced: 0, biasChanged: false };
  }

  const prevBias = String(st.lastBias || "");
  const biasChanged = prevBias !== "" && prevBias !== bias;

  if (REBALANCE_ONLY_ON_BIAS_CHANGE && !biasChanged) {
    return { buyReplaced: 0, sellReplaced: 0, biasChanged: false };
  }

  const { buyList, sellList } = extractOrdersAnyShape(openOrders);

  let idrFree = 0;
  let dgbFree = 0;
  if (infoAfterCancel) {
    idrFree = Number(infoAfterCancel.balance?.idr || 0);
    dgbFree = Number(infoAfterCancel.balance?.dgb || 0);
  } else {
    try {
      const info = await api.getInfo(env, nonceCtx);
      idrFree = Number(info.balance?.idr || 0);
      dgbFree = Number(info.balance?.dgb || 0);
    } catch {}
  }

  const nearestBuys = buyList
    .filter(o => {
      const p = orderPrice(o);
      return p > 0 && !isBuyLocked(st, p);
    })
    .sort((a, b) => Math.abs(orderPrice(a) - last) - Math.abs(orderPrice(b) - last))
    .slice(0, REBALANCE_NEAR_COUNT);

  const nearestSells = sellList
    .filter(o => {
      const p = orderPrice(o);
      return p > 0 && !isSellLocked(st, p);
    })
    .sort((a, b) => Math.abs(orderPrice(a) - last) - Math.abs(orderPrice(b) - last))
    .slice(0, REBALANCE_NEAR_COUNT);

  let buyReplaced = 0;
  let sellReplaced = 0;

  for (const o of nearestBuys) {
    if (buyReplaced >= REBALANCE_MAX_BUY_PER_RUN) break;

    const price = orderPrice(o);
    const oldQty = orderRemainingQty(o);
    const targetIdr = dynamicBuyAmountAdaptive(price, bias, marketState);
    const idealQty = price > 0 ? (targetIdr / price) : 0;

    if (idealQty <= 0 || oldQty <= 0) continue;
    if (pctDiff(oldQty, idealQty) < REBALANCE_DIFF_PCT) continue;

    try {
      await api.cancelOrder(env, GRID_PAIR_TAPI, orderId(o), "buy", nonceCtx);

      const refreshed = await api.getInfo(env, nonceCtx);
      idrFree = Number(refreshed.balance?.idr || 0);

      if ((idrFree - targetIdr) < MIN_IDR_RESERVE) continue;

      await api.trade(env, GRID_PAIR_TAPI, "buy", price, { idr: String(targetIdr) }, nonceCtx);
      buyReplaced++;
      idrFree -= targetIdr;
    } catch {}
  }

  for (const o of nearestSells) {
    if (sellReplaced >= REBALANCE_MAX_SELL_PER_RUN) break;

    const price = orderPrice(o);
    const oldQty = orderRemainingQty(o);
    const targetIdr = dynamicSellTargetAdaptive(price, bias, marketState);
    const idealQty = calcSellQty(price, targetIdr);

    if (idealQty <= 0 || oldQty <= 0) continue;
    if (pctDiff(oldQty, idealQty) < REBALANCE_DIFF_PCT) continue;

    try {
      await api.cancelOrder(env, GRID_PAIR_TAPI, orderId(o), "sell", nonceCtx);

      const refreshed = await api.getInfo(env, nonceCtx);
      dgbFree = Number(refreshed.balance?.dgb || 0);

      if ((dgbFree - idealQty) < MIN_DGB_RESERVE) continue;

      await api.trade(env, GRID_PAIR_TAPI, "sell", price, { dgb: String(idealQty) }, nonceCtx);
      sellReplaced++;
      dgbFree -= idealQty;
    } catch {}
  }

  return { buyReplaced, sellReplaced, biasChanged };
}

function pctDiff(a, b) {
  const x = Number(a || 0);
  const y = Number(b || 0);
  if (x <= 0 && y <= 0) return 0;
  if (y <= 0) return 1;
  return Math.abs(x - y) / y;
}

function generateActiveGridLevels(last, bias, marketState, cfg) {
  const buyLevels = [];
  const sellLevels = [];

  const isSideways = !!marketState?.isSideways;
  const workRange = isSideways ? cfg.microActiveRange : cfg.activeRange;

  const minP = Math.max(cfg.gridRangeLow, last - workRange);
  const maxP = Math.min(cfg.gridRangeHigh, last + workRange);

  for (let p = minP; p <= maxP; p++) {
    if (p < last && isAllowedBuyPrice(p)) {
      if (!isSideways || pairSpreadPctFromBuy(p) >= MICRO_MIN_SPREAD_PCT) {
        buyLevels.push(p);
      }
    }

    if (p > last && isAllowedSellPrice(p)) {
      if (!isSideways || pairSpreadPctFromSell(p) >= MICRO_MIN_SPREAD_PCT) {
        sellLevels.push(p);
      }
    }
  }

  buyLevels.sort((a, b) => b - a);
  sellLevels.sort((a, b) => a - b);

  return { buyLevels, sellLevels };
}

function isAllowedBuyPrice(price) {
  return BUY_TO_SELL.has(Number(price));
}

function isAllowedSellPrice(price) {
  return SELL_TO_BUY.has(Number(price));
}

async function autoCancelFarOrders(env, st, last, nonceCtx, marketState = null, cfg) {
  const oo = await api.openOrders(env, GRID_PAIR_TAPI, nonceCtx);
  const { buyList, sellList } = extractOrdersAnyShape(oo);

  let cancelOk = 0;
  let cancelFail = 0;

  const isSideways = !!marketState?.isSideways;
  const workRange = isSideways ? cfg.microActiveRange : cfg.activeRange;

  for (const o of [...buyList, ...sellList]) {
    if (cancelOk >= MAX_CANCEL_PER_RUN) break;

    const price = orderPrice(o);
    const side = orderSide(o);

    if (side === "buy" && isBuyLocked(st, price)) continue;
    if (side === "sell" && isSellLocked(st, price)) continue;

    let mustCancel = false;

    if (Math.abs(price - last) > workRange) mustCancel = true;

    if (!isSideways) {
      if (side === "buy" && price >= last) mustCancel = true;
      if (side === "sell" && price <= last) mustCancel = true;
    } else {
      if (side === "buy" && price > last) mustCancel = true;
      if (side === "sell" && price < last) mustCancel = true;
    }

    if (side === "sell" && price > cfg.gridRangeHigh) mustCancel = true;
    if (side === "buy" && price < cfg.gridRangeLow) mustCancel = true;

    if (!isSideways) {
      if (side === "sell" && price > last + 12) mustCancel = true;
      if (side === "buy" && price < last - 16) mustCancel = true;
    }

    if (price < cfg.gridRangeLow || price > cfg.gridRangeHigh) mustCancel = true;

    if (side === "buy" && !isAllowedBuyPrice(price)) mustCancel = true;
    if (side === "sell" && !isAllowedSellPrice(price)) mustCancel = true;

    if (!mustCancel) continue;

    try {
      await api.cancelOrder(env, GRID_PAIR_TAPI, orderId(o), side, nonceCtx);
      cancelOk++;
    } catch {
      cancelFail++;
    }
  }

  return { cancelOk, cancelFail };
}

async function autoGenerateOrders(env, st, last, idrFreeStart, dgbFreeStart, bias, nonceCtx, marketState = null, cfg, coinRatio = 0, targetCoinRatio = 0) {
  let idrFree = Number(idrFreeStart || 0);
  let dgbFree = Number(dgbFreeStart || 0);

  const isSideways = !!marketState?.isSideways;
  const levels = generateActiveGridLevels(last, bias, marketState, cfg);

  let buyPlaced = 0;
  let sellPlaced = 0;
  let buySkipped = 0;
  let sellSkipped = 0;
  let buySkippedInventory = 0;
  let sellSkippedInventory = 0;
  let buySkippedVolume = 0;
  let sellSkippedVolume = 0;

  const maxNewBuy = isSideways ? MICRO_MAX_NEW_BUY_PER_RUN : MAX_NEW_BUY_PER_RUN;
  const maxNewSell = isSideways ? MICRO_MAX_NEW_SELL_PER_RUN : MAX_NEW_SELL_PER_RUN;
  const buyNearGap = isSideways ? MICRO_NEAR_GAP : cfg.buyNearGap;
  const sellNearGap = isSideways ? MICRO_NEAR_GAP : cfg.sellNearGap;

  let oo = await api.openOrders(env, GRID_PAIR_TAPI, nonceCtx);

  for (const price of levels.buyLevels) {
    if (buyPlaced >= maxNewBuy) break;

    if (isBuyLocked(st, price)) {
      buySkipped++;
      continue;
    }

    if (hasOrderAt(oo, "buy", price)) continue;

    if (price > (last - buyNearGap)) {
      buySkipped++;
      continue;
    }

    if (!isSideways && bias === "SELL" && price > last - 4) {
      buySkipped++;
      continue;
    }

    if (isSideways && bias === "SELL" && coinRatio > targetCoinRatio + INVENTORY_BIAS_TOLERANCE) {
      buySkipped++;
      buySkippedInventory++;
      continue;
    }

    // Volume confirmation
    const volume24h = await getVolume24h(GRID_PAIR_PUBLIC);
    if (volume24h < MIN_VOLUME_24H_IDR) {
      buySkipped++;
      buySkippedVolume++;
      continue;
    }

    const amount = dynamicBuyAmountAdaptive(price, bias, marketState);
    if (amount <= 0) {
      buySkipped++;
      continue;
    }

    if ((idrFree - amount) < MIN_IDR_RESERVE) {
      buySkipped++;
      continue;
    }

    try {
      await api.trade(env, GRID_PAIR_TAPI, "buy", price, { idr: String(amount) }, nonceCtx);
      buyPlaced++;
      idrFree -= amount;
      oo = await api.openOrders(env, GRID_PAIR_TAPI, nonceCtx);
    } catch {
      buySkipped++;
    }
  }

  for (const price of levels.sellLevels) {
    if (sellPlaced >= maxNewSell) break;

    if (isSellLocked(st, price)) {
      sellSkipped++;
      continue;
    }

    if (hasOrderAt(oo, "sell", price)) continue;

    if (price < (last + sellNearGap)) {
      sellSkipped++;
      continue;
    }

    if (!isSideways && bias === "BUY" && price < last + 4) {
      sellSkipped++;
      continue;
    }

    if (isSideways && bias === "BUY" && coinRatio < targetCoinRatio - INVENTORY_BIAS_TOLERANCE) {
      sellSkipped++;
      sellSkippedInventory++;
      continue;
    }

    // Volume confirmation
    const volume24hSell = await getVolume24h(GRID_PAIR_PUBLIC);
    if (volume24hSell < MIN_VOLUME_24H_IDR) {
      sellSkipped++;
      sellSkippedVolume++;
      continue;
    }

    const target = dynamicSellTargetAdaptive(price, bias, marketState);
    const qty = calcSellQty(price, target);

    if (qty <= 0) {
      sellSkipped++;
      continue;
    }

    if ((dgbFree - qty) < MIN_DGB_RESERVE) {
      sellSkipped++;
      continue;
    }

    try {
      await api.trade(env, GRID_PAIR_TAPI, "sell", price, { dgb: String(qty) }, nonceCtx);
      sellPlaced++;
      dgbFree -= qty;
      oo = await api.openOrders(env, GRID_PAIR_TAPI, nonceCtx);
    } catch {
      sellSkipped++;
    }
  }

  return { buyPlaced, sellPlaced, buySkipped, sellSkipped, buySkippedInventory, sellSkippedInventory, buySkippedVolume, sellSkippedVolume };
}

// ================= HELPERS =================
function ensureStateShape(st) {
  if (!st.pairSide || typeof st.pairSide !== "object") st.pairSide = {};
  if (!st.lockedBuys || typeof st.lockedBuys !== "object") st.lockedBuys = {};
  if (!st.lockedSells || typeof st.lockedSells !== "object") st.lockedSells = {};
  if (!("lastTradeSig" in st)) st.lastTradeSig = "";
  if (!("assumedEntryPrice" in st)) st.assumedEntryPrice = ASSUMED_ENTRY_PRICE_DEFAULT;
  if (!("autoGridEnabled" in st)) st.autoGridEnabled = true;
  if (!("lastBias" in st)) st.lastBias = "";
  if (!("manualPaused" in st)) st.manualPaused = false;
  if (!("gridRangeLow" in st)) st.gridRangeLow = DEFAULT_GRID_RANGE_LOW;
  if (!("gridRangeHigh" in st)) st.gridRangeHigh = DEFAULT_GRID_RANGE_HIGH;
  if (!("activeRange" in st)) st.activeRange = DEFAULT_ACTIVE_RANGE;
  if (!("microActiveRange" in st)) st.microActiveRange = DEFAULT_MICRO_ACTIVE_RANGE;
  if (!("buyNearGap" in st)) st.buyNearGap = DEFAULT_BUY_NEAR_GAP;
  if (!("sellNearGap" in st)) st.sellNearGap = DEFAULT_SELL_NEAR_GAP;
}

function defaultPairSide(buyP, assumedEntry) {
  return buyP < Math.round(assumedEntry || ASSUMED_ENTRY_PRICE_DEFAULT) ? "buy" : "sell";
}

function seedInitialPairStates(st) {
  ensureStateShape(st);
  const assumed = Math.round(st.assumedEntryPrice || ASSUMED_ENTRY_PRICE_DEFAULT);
  const obj = {};
  for (const buyP of BUY_TO_SELL.keys()) {
    obj[String(buyP)] = defaultPairSide(buyP, assumed);
  }
  st.pairSide = obj;
}

function formatPairStateDetail(st) {
  return Array.from(BUY_TO_SELL.entries()).map(([b,s]) => {
    const side = st.pairSide?.[String(b)] || defaultPairSide(b, st.assumedEntryPrice);
    const lb = isBuyLocked(st, b) ? " [LOCK BUY]" : "";
    const ls = isSellLocked(st, s) ? " [LOCK SELL]" : "";
    return `• ${b}->${s} : ${side.toUpperCase()}${lb}${ls}`;
  }).join("\n");
}

function isBuyLocked(st, buyP) {
  return !!st.lockedBuys?.[String(buyP)];
}

function isSellLocked(st, sellP) {
  return !!st.lockedSells?.[String(sellP)];
}

function formatBuyLocks(st) {
  const xs = Object.keys(st.lockedBuys || {})
    .map(Number)
    .filter(x => Number.isFinite(x) && x > 0)
    .sort((a,b)=>a-b);
  return xs.join(", ") || "-";
}

function formatSellLocks(st) {
  const xs = Object.keys(st.lockedSells || {})
    .map(Number)
    .filter(x => Number.isFinite(x) && x > 0)
    .sort((a,b)=>a-b);
  return xs.join(", ") || "-";
}

function fmtQty(n) {
  const x = Number(n || 0);
  if (!Number.isFinite(x)) return "0";
  if (Math.abs(x - Math.round(x)) < 1e-9) return Math.round(x).toLocaleString("id-ID");
  return x.toLocaleString("id-ID", { maximumFractionDigits: 8 });
}

function findNearestAllowedBuy(lastPrice) {
  const candidates = Array.from(BUY_TO_SELL.keys());
  let best = 0;
  let bestDist = Infinity;
  for (const p of candidates) {
    const d = Math.abs(p - lastPrice);
    if (d < bestDist) {
      best = p;
      bestDist = d;
    }
  }
  return best;
}

function findNearestAllowedSell(lastPrice) {
  const candidates = Array.from(SELL_TO_BUY.keys());
  let best = 0;
  let bestDist = Infinity;
  for (const p of candidates) {
    const d = Math.abs(p - lastPrice);
    if (d < bestDist) {
      best = p;
      bestDist = d;
    }
  }
  return best;
}

function pickFirstFinitePositive(values, allowZero = true) {
  for (const v of values) {
    const n = Number(v);
    if (!Number.isFinite(n)) continue;
    if (allowZero ? n >= 0 : n > 0) return n;
  }
  return NaN;
}

function summarizeOrdersByPrice(list, side) {
  const m = new Map();

  for (const o of list) {
    const p = orderPrice(o);
    if (!p) continue;

    const qty = orderRemainingQty(o);
    const prev = m.get(p) || { count: 0, qty: 0 };
    prev.count += 1;
    prev.qty += qty;
    m.set(p, prev);
  }

  const arr = [...m.entries()].map(([price, v]) => ({
    price,
    count: v.count,
    qty: v.qty
  }));

  arr.sort((a, b) => side === "buy" ? b.price - a.price : a.price - b.price);

  return arr.map(x => {
    const qtyTxt = fmtQty(x.qty);
    const countTxt = x.count > 1 ? ` x${x.count}` : "";
    return `${x.price}(${qtyTxt}${countTxt})`;
  });
}

function splitText(text, maxLen) {
  const out = [];
  let s = String(text || "");
  while (s.length > maxLen) {
    out.push(s.slice(0, maxLen));
    s = s.slice(maxLen);
  }
  if (s.length) out.push(s);
  return out.length ? out : [""];
}

// ================= PUBLIC TRADES =================
async function getTrades(pair) {
  const url = `https://indodax.com/api/trades/${pair.toLowerCase()}`;
  const trades = await fetch(url).then(r => r.json());
  if (!Array.isArray(trades)) return null;
  return trades;
}

async function getVolume24h(pair) {
  try {
    const url = `https://indodax.com/api/ticker/${pair.toLowerCase()}`;
    const data = await fetch(url).then(r => r.json());
    if (data && data.ticker && data.ticker.vol_idr) {
      return Number(data.ticker.vol_idr);
    }
  } catch {}
  return 0;
}

// ================= STATE =================
async function getState(env) {
  const raw = await env.STATE.get(STATE_KEY);
  if (!raw) {
    const st = {
      lastTradeTs: 0,
      lastTradeSig: "",
      lastReportTs: 0,
      lastReportedPrice: 0,
      assumedEntryPrice: ASSUMED_ENTRY_PRICE_DEFAULT,
      manualStopped: false,
      manualPaused: false,
      autoGridEnabled: true,
      lastBias: "",
      pairSide: {},
      lockedBuys: {},
      lockedSells: {},
      gridRangeLow: DEFAULT_GRID_RANGE_LOW,
      gridRangeHigh: DEFAULT_GRID_RANGE_HIGH,
      activeRange: DEFAULT_ACTIVE_RANGE,
      microActiveRange: DEFAULT_MICRO_ACTIVE_RANGE,
      buyNearGap: DEFAULT_BUY_NEAR_GAP,
      sellNearGap: DEFAULT_SELL_NEAR_GAP
    };
    seedInitialPairStates(st);
    return st;
  }

  try {
    const j = JSON.parse(raw);
    const st = {
      lastTradeTs: Number(j.lastTradeTs || 0),
      lastTradeSig: String(j.lastTradeSig || ""),
      lastReportTs: Number(j.lastReportTs || 0),
      lastReportedPrice: Number(j.lastReportedPrice || 0),
      assumedEntryPrice: Number(j.assumedEntryPrice || ASSUMED_ENTRY_PRICE_DEFAULT),
      manualStopped: j.manualStopped === true,
      autoGridEnabled: j.autoGridEnabled !== false,
      lastBias: String(j.lastBias || ""),
      manualPaused: j.manualPaused === true,
      pairSide: (j.pairSide && typeof j.pairSide === "object") ? j.pairSide : {},
      lockedBuys: (j.lockedBuys && typeof j.lockedBuys === "object") ? j.lockedBuys : {},
      lockedSells: (j.lockedSells && typeof j.lockedSells === "object") ? j.lockedSells : {},
      gridRangeLow: Number(j.gridRangeLow || DEFAULT_GRID_RANGE_LOW),
      gridRangeHigh: Number(j.gridRangeHigh || DEFAULT_GRID_RANGE_HIGH),
      activeRange: Number(j.activeRange || DEFAULT_ACTIVE_RANGE),
      microActiveRange: Number(j.microActiveRange || DEFAULT_MICRO_ACTIVE_RANGE),
      buyNearGap: Number(j.buyNearGap || DEFAULT_BUY_NEAR_GAP),
      sellNearGap: Number(j.sellNearGap || DEFAULT_SELL_NEAR_GAP)
    };
    ensureStateShape(st);
    return st;
  } catch {
    const st = {
      lastTradeTs: 0,
      lastTradeSig: "",
      lastReportTs: 0,
      lastReportedPrice: 0,
      assumedEntryPrice: ASSUMED_ENTRY_PRICE_DEFAULT,
      manualStopped: false,
      manualPaused: false,
      autoGridEnabled: true,
      lastBias: "",
      pairSide: {},
      lockedBuys: {},
      lockedSells: {},
      gridRangeLow: DEFAULT_GRID_RANGE_LOW,
      gridRangeHigh: DEFAULT_GRID_RANGE_HIGH,
      activeRange: DEFAULT_ACTIVE_RANGE,
      microActiveRange: DEFAULT_MICRO_ACTIVE_RANGE,
      buyNearGap: DEFAULT_BUY_NEAR_GAP,
      sellNearGap: DEFAULT_SELL_NEAR_GAP
    };
    seedInitialPairStates(st);
    return st;
  }
}

async function setState(env, st) {
  await env.STATE.put(STATE_KEY, JSON.stringify(st));
}

async function shouldSendPriceUpdate(env, lastPrice) {
  const raw = await env.STATE.get(LAST_PRICE_STATE_KEY);
  let prev = 0;

  if (raw) {
    try {
      const j = JSON.parse(raw);
      prev = Number(j.last || 0);
    } catch {
      prev = 0;
    }
  }

  if (!prev) {
    await env.STATE.put(LAST_PRICE_STATE_KEY, JSON.stringify({
      last: lastPrice,
      ts: Date.now()
    }));
    return true;
  }

  if (prev !== lastPrice) {
    await env.STATE.put(LAST_PRICE_STATE_KEY, JSON.stringify({
      last: lastPrice,
      ts: Date.now()
    }));
    return true;
  }

  return false;
}

// ================= RUN LOCK =================
function nowSec() {
  return Math.floor(Date.now() / 1000);
}

async function acquireRunLock(env) {
  const ts = nowSec();
  try {
    const raw = await env.STATE.get(RUN_LOCK_KEY);
    const last = Number(raw || 0);
    if (last && (ts - last) < RUN_LOCK_SEC) return false;
    await env.STATE.put(RUN_LOCK_KEY, String(ts), { expirationTtl: RUN_LOCK_SEC });
    return true;
  } catch {
    return true;
  }
}

// ================= TRADE HISTORY =================
function tradeSig(t) {
  const sideRaw = String(t?.type || t?.side || "").toLowerCase();
  const side = sideRaw.includes("sell") ? "sell" : "buy";
  const price = Math.round(Number(t?.price || t?.rate || t?.order_price || 0));
  const qty = Number(t?.amount || t?.qty || t?.dgb || t?.btc || 0);
  const ts = toEpochSec(t?.trade_time ?? t?.timestamp ?? t?.time ?? t?.date);
  const oid = String(t?.order_id || t?.orderId || "");
  const tid = String(t?.trade_id || t?.id || "");
  return `${ts}|${side}|${price}|${qty}|${oid}|${tid}`;
}

async function getNewestTradeMark(env, nonceCtx) {
  try {
    const th = await api.tradeHistory(env, GRID_PAIR_TAPI, 200, nonceCtx);
    const tradesRaw =
      normalizeList(th?.trades) ||
      normalizeList(th?.return?.trades) ||
      normalizeList(th);

    const parsed = tradesRaw.map(t => ({
      ts: toEpochSec(t?.trade_time ?? t?.timestamp ?? t?.time ?? t?.date),
      sig: tradeSig(t)
    })).filter(x => x.ts > 0);

    if (!parsed.length) return { ts: 0, sig: "" };
    parsed.sort((a, b) => a.ts - b.ts || a.sig.localeCompare(b.sig));
    return parsed[parsed.length - 1];
  } catch {
    return { ts: 0, sig: "" };
  }
}

async function readNewTradesRaw(env, nonceCtx, lastTs, lastSig) {
  let th;
  try {
    th = await api.tradeHistory(env, GRID_PAIR_TAPI, 200, nonceCtx);
  } catch {
    return { items: [], newestTs: lastTs || 0, newestSig: lastSig || "" };
  }

  const tradesRaw =
    normalizeList(th?.trades) ||
    normalizeList(th?.return?.trades) ||
    normalizeList(th);

  const now = nowSec();

  const parsed = tradesRaw.map(t => {
    const sideRaw = String(t?.type || t?.side || "").toLowerCase();
    const side = sideRaw.includes("sell") ? "sell" : "buy";
    const price = Math.round(Number(t?.price || t?.rate || t?.order_price || 0));
    const ts = toEpochSec(t?.trade_time ?? t?.timestamp ?? t?.time ?? t?.date);
    const sig = tradeSig(t);
    return { side, price, ts, sig };
  }).filter(x =>
    x.price > 0 &&
    x.ts > 0 &&
    x.ts <= now + 60 &&
    (now - x.ts) <= FILL_RECENCY_SEC
  );

  if (!parsed.length) {
    return {
      items: [],
      newestTs: Number(lastTs || 0),
      newestSig: String(lastSig || "")
    };
  }

  parsed.sort((a, b) => a.ts - b.ts || a.sig.localeCompare(b.sig));

  const items = parsed.filter(x => {
    if (x.ts > Number(lastTs || 0)) return true;
    if (x.ts === Number(lastTs || 0) && x.sig > String(lastSig || "")) return true;
    return false;
  });

  const lastItem = parsed[parsed.length - 1];
  return { items, newestTs: lastItem.ts, newestSig: lastItem.sig };
}

// ================= TIMESTAMP =================
function toEpochSec(v) {
  if (v == null) return 0;

  if (typeof v === "number") {
    if (!Number.isFinite(v)) return 0;
    if (v > 1e12) return Math.floor(v / 1000);
    if (v > 1e9) return Math.floor(v);
    return 0;
  }

  const s = String(v).trim();

  if (/^\d+$/.test(s)) {
    const n = Number(s);
    if (!Number.isFinite(n)) return 0;
    if (n > 1e12) return Math.floor(n / 1000);
    if (n > 1e9) return Math.floor(n);
    return 0;
  }

  const m = s.match(/^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})(?::(\d{2}))?/);
  if (m) {
    const [_, yy, mo, dd, hh, mi, ss] = m;
    const dt = Date.UTC(+yy, +mo - 1, +dd, +hh - 7, +mi, +(ss || 0));
    return Math.floor(dt / 1000);
  }

  const t = Date.parse(s);
  if (!Number.isFinite(t)) return 0;
  return Math.floor(t / 1000);
}

// ================= ORDER HELPERS =================
function normalizeList(x) {
  if (!x) return [];
  if (Array.isArray(x)) return x;
  if (typeof x === "object") return Object.values(x);
  return [];
}

function orderSide(o) {
  const t = String(o?.type ?? o?.side ?? o?.order_type ?? o?.orderType ?? "").toLowerCase();
  if (t.includes("buy")) return "buy";
  if (t.includes("sell")) return "sell";
  return "";
}

function orderPrice(o) {
  const p = o?.price ?? o?.rate ?? o?.order_price ?? o?.orderPrice ?? 0;
  const n = Number(p);
  return Number.isFinite(n) ? Math.round(n) : 0;
}

function orderId(o) {
  return String(o?.order_id ?? o?.orderId ?? o?.id ?? "");
}

function orderRemainingQty(o) {
  const side = orderSide(o);
  const price = orderPrice(o);

  const qtyDirect = pickFirstFinitePositive([
    o?.remain_amount,
    o?.remaining_amount,
    o?.rest_amount,
    o?.order_qty,
    o?.amount,
    o?.qty,
    o?.remain_qty,
    o?.remaining_qty,
    o?.rest_qty,
    o?.order_dgb,
    o?.dgb,
    o?.remain_dgb,
    o?.remaining_dgb
  ], false);

  if (Number.isFinite(qtyDirect) && qtyDirect > 0) return qtyDirect;

  if (side === "buy" && price > 0) {
    const idrRemain = pickFirstFinitePositive([
      o?.remain_rp,
      o?.remaining_rp,
      o?.rest_rp,
      o?.remain_idr,
      o?.remaining_idr,
      o?.idr
    ], false);

    if (Number.isFinite(idrRemain) && idrRemain > 0) {
      return idrRemain / price;
    }

    const idrOrder = pickFirstFinitePositive([
      o?.order_rp
    ], false);

    if (Number.isFinite(idrOrder) && idrOrder > 0) {
      return idrOrder / price;
    }
  }

  if (side === "sell") {
    const sellQty = pickFirstFinitePositive([
      o?.order_dgb,
      o?.dgb,
      o?.amount,
      o?.qty
    ], false);

    if (Number.isFinite(sellQty) && sellQty > 0) {
      return sellQty;
    }
  }

  return 0;
}

function extractOrdersAnyShape(oo) {
  const ordersList = normalizeList(oo?.orders ?? oo?.return?.orders ?? null);
  if (ordersList.length) {
    const buyList = [];
    const sellList = [];
    for (const o of ordersList) {
      const s = orderSide(o);
      if (s === "buy") buyList.push(o);
      else if (s === "sell") sellList.push(o);
    }
    return { buyList, sellList, all: ordersList };
  }

  const buyList = normalizeList(oo?.buy ?? oo?.return?.buy ?? null);
  const sellList = normalizeList(oo?.sell ?? oo?.return?.sell ?? null);
  return { buyList, sellList, all: [...buyList, ...sellList] };
}

function hasOrderAt(openOrders, side, price) {
  const { buyList, sellList } = extractOrdersAnyShape(openOrders);
  const list = side === "buy" ? buyList : sellList;
  return list.some(o => orderPrice(o) === Number(price));
}

function roundTo(n, step) {
  const s = Math.max(1, Number(step || 1));
  return Math.round(Number(n || 0) / s) * s;
}

function calcSellQty(price, idrTarget) {
  const p = Math.max(1, Number(price || 1));
  const idr = Math.max(0, Number(idrTarget || 0));
  return Math.max(0, Math.floor(idr / p));
}

function fmtWIBTime() {
  const d = new Date(Date.now() + 7 * 3600 * 1000);
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  const hh = String(d.getUTCHours()).padStart(2, "0");
  const mi = String(d.getUTCMinutes()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd} ${hh}:${mi} WIB`;
}

// ================= INDODAX TAPI + NONCE =================
const TAPI_URL = "https://indodax.com/tapi";
const NONCE_STATE_KEY = "NONCE_STATE_INDODAX_FULL_MANUAL_V35";

async function hmacSha512Hex(secret, msg) {
  const enc = new TextEncoder();
  const key = await crypto.subtle.importKey(
    "raw",
    enc.encode(secret),
    { name: "HMAC", hash: "SHA-512" },
    false,
    ["sign"]
  );
  const sig = await crypto.subtle.sign("HMAC", key, enc.encode(msg));
  return [...new Uint8Array(sig)].map(b => b.toString(16).padStart(2, "0")).join("");
}

async function getNonceState(env) {
  try {
    const raw = await env.STATE.get(NONCE_STATE_KEY);
    if (!raw) return { last: 0 };
    const j = JSON.parse(raw);
    const last = Number(j.last || 0);
    return { last: Number.isFinite(last) ? last : 0 };
  } catch {
    return { last: 0 };
  }
}

async function setNonceState(env, last) {
  try {
    await env.STATE.put(NONCE_STATE_KEY, JSON.stringify({ last: Number(last) }));
  } catch {}
}

let _memLastNonce = 0;
let _memInc = 0;

async function createNonceCtx(env) {
  const st = await getNonceState(env);
  let last = Math.max(Number(st.last || 0), Number(_memLastNonce || 0));

  function next() {
    const now = Date.now();
    let candidate = now * 1000;

    const memBase = Math.floor(_memLastNonce / 1000) * 1000;
    if (candidate === memBase) candidate = candidate + (_memInc++);
    else _memInc = 0;

    if (candidate <= last) candidate = last + 1;

    last = candidate;
    _memLastNonce = candidate;
    return String(candidate);
  }

  async function flush() {
    await setNonceState(env, last);
  }

  return { next, flush };
}

async function tapi(env, method, params = {}, nonceCtx) {
  const nonce = nonceCtx ? nonceCtx.next() : String(Date.now() * 1000);
  const body = new URLSearchParams({ method, nonce, ...params }).toString();
  const sign = await hmacSha512Hex(env.INDODAX_SECRET, body);

  const r = await fetch(TAPI_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
      "Key": env.INDODAX_KEY,
      "Sign": sign
    },
    body
  });

  const j = await r.json();
  if (j && j.success === 1) return j.return;
  throw new Error(`TAPI error: ${JSON.stringify(j)}`);
}

const api = {
  getInfo: (env, nonceCtx) => tapi(env, "getInfo", {}, nonceCtx),
  openOrders: (env, pair, nonceCtx) => tapi(env, "openOrders", { pair }, nonceCtx),
  tradeHistory: (env, pair, count = 50, nonceCtx) =>
    tapi(env, "tradeHistory", { pair, count: String(count), order: "asc" }, nonceCtx),
  trade: (env, pair, type, price, extra, nonceCtx) =>
    tapi(env, "trade", { pair, type, price: String(price), ...extra }, nonceCtx),
  cancelOrder: (env, pair, order_id, type, nonceCtx) =>
    tapi(env, "cancelOrder", { pair, order_id: String(order_id), type: String(type) }, nonceCtx),
};

// ================= TELEGRAM SEND =================
async function sendMsg(env, chatId, text) {
  await fetch(`https://api.telegram.org/bot${env.BOT_TOKEN}/sendMessage`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ chat_id: chatId, text })
  });
}