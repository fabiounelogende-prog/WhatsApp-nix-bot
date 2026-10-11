// Nix Bot
// Un seul fichier : connecte ton numéro WhatsApp (code de jumelage),
// garde la session, et relaie les messages à des sous-bots via WebSocket.
//
// Installation : npm i @whiskeysockets/baileys pino ws
// Lancement   : node nix.js

const {
  default: makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion,
} = require("@whiskeysockets/baileys");
const pino = require("pino");
const { WebSocketServer } = require("ws");

// ───────── CONFIG ─────────
const config = {
  botName: "Nix Bot",
  numero: "243810995981",   // ton numéro, sans + ni espaces
  wsPort: 8787,              // port local pour les sous-bots
  authKey: "change-moi",     // les sous-bots doivent envoyer cette clé pour se connecter
};

const subBots = new Map(); // id -> ws

async function start() {
  const { state, saveCreds } = await useMultiFileAuthState("./session");
  const { version } = await fetchLatestBaileysVersion();

  const sock = makeWASocket({
    version,
    auth: state,
    logger: pino({ level: "silent" }),
    printQRInTerminal: false,
    browser: ["Ubuntu", "Chrome", "20.0.04"],
  });

  sock.ev.on("creds.update", saveCreds);

  // ───────── Connexion (code de jumelage) ─────────
  if (!sock.authState.creds.registered) {
    console.log(`📲 Demande d'un code de jumelage pour ${config.numero}...`);
    try {
      const code = await sock.requestPairingCode(config.numero.replace(/\D/g, ""));
      console.log("");
      console.log("═══════════════════════════════");
      console.log(`   CODE DE JUMELAGE : ${code}`);
      console.log("═══════════════════════════════");
      console.log("");
      console.log("Sur ton téléphone :");
      console.log("WhatsApp > Réglages > Appareils connectés");
      console.log("> Connecter un appareil > Connecter avec un numéro de téléphone");
      console.log("puis entre ce code.");
    } catch (e) {
      console.error("❌ Impossible de générer le code :", e.message);
      process.exit(1);
    }
  }

  sock.ev.on("connection.update", ({ connection, lastDisconnect }) => {
    if (connection === "open") {
      console.log(`✅ ${config.botName} connecté à WhatsApp`);
    }
    if (connection === "close") {
      const code = lastDisconnect?.error?.output?.statusCode;
      if (code === DisconnectReason.loggedOut) {
        console.log("❌ Session expirée : supprime ./session et relance.");
      } else {
        console.log("🔄 Reconnexion...");
        start();
      }
    }
  });

  // ───────── Messages WhatsApp entrants → diffusés aux sous-bots ─────────
  sock.ev.on("messages.upsert", async ({ messages }) => {
    for (const msg of messages) {
      if (!msg.message) continue;
      const text =
        msg.message.conversation || msg.message.extendedTextMessage?.text || "";
      const payload = JSON.stringify({
        type: "message",
        from: msg.key.remoteJid,
        fromMe: msg.key.fromMe,
        text,
      });
      for (const ws of subBots.values()) {
        if (ws.readyState === 1) ws.send(payload);
      }
    }
  });

  // ───────── Serveur WebSocket local pour les sous-bots ─────────
  const wss = new WebSocketServer({ port: config.wsPort });
  console.log(`🔌 ${config.botName} — WebSocket en écoute sur ws://localhost:${config.wsPort}`);

  wss.on("connection", (ws) => {
    let id = null;

    ws.on("message", async (raw) => {
      let data;
      try { data = JSON.parse(raw); } catch { return; }

      // Un sous-bot doit d'abord s'authentifier
      if (data.type === "auth") {
        if (data.key !== config.authKey) {
          ws.send(JSON.stringify({ type: "error", message: "Clé invalide" }));
          return ws.close();
        }
        id = data.id || `subbot-${Date.now()}`;
        subBots.set(id, ws);
        console.log(`➕ Sous-bot connecté : ${id}`);
        ws.send(JSON.stringify({ type: "ready", id }));
        return;
      }

      if (!id) return; // pas encore authentifié

      // Un sous-bot demande l'envoi d'un message WhatsApp
      if (data.type === "send" && data.to && data.text) {
        try {
          await sock.sendMessage(data.to, { text: data.text });
        } catch (e) {
          ws.send(JSON.stringify({ type: "error", message: e.message }));
        }
      }
    });

    ws.on("close", () => {
      if (id) {
        subBots.delete(id);
        console.log(`➖ Sous-bot déconnecté : ${id}`);
      }
    });
  });
}

start();
