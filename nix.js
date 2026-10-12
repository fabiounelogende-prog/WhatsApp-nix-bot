// Nix Bot
// Un seul fichier : connecte ton numéro WhatsApp (code de jumelage),
// garde la session, et relaie les messages à des sous-bots via WebSocket.
//
// Installation : npm i @whiskeysockets/baileys pino ws
// Lancement   : node nix.js
//
// ⚠️ IMPORTANT (Render / hébergeurs à disque éphémère) :
// Sur le plan gratuit de Render, le système de fichiers est effacé à
// chaque redéploiement ET à chaque réveil après une période d'inactivité.
// Le dossier ./session (qui contient tes identifiants WhatsApp) disparaît
// alors à chaque fois, ce qui oblige à refaire un jumelage en boucle et
// provoque l'erreur "Session expirée" vue dans les logs.
//
// Deux solutions :
//   1) Ajouter un "Persistent Disk" à ton service Render (payant, même
//      minime) et monter SESSION_DIR dessus (ex: /var/data/session),
//      défini via la variable d'environnement SESSION_DIR.
//   2) Héberger ailleurs qu'un plan gratuit à disque éphémère (un VPS,
//      un Raspberry Pi chez toi, etc.) où ./session persiste normalement.
// Sans l'un des deux, ce bot devra être re-jumelé à chaque redémarrage.

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
  wsPort: process.env.WS_PORT || 8787,
  authKey: process.env.AUTH_KEY || "change-moi",
  // Permet de pointer vers un disque persistant monté (voir note ci-dessus)
  sessionDir: process.env.SESSION_DIR || "./session",
};

if (config.authKey === "change-moi") {
  console.warn("⚠️  AUTH_KEY n'a pas été changé — défini une vraie clé via la variable d'environnement AUTH_KEY avant de passer en production.");
}

const subBots = new Map(); // id -> ws
let reconnectAttempts = 0;
const MAX_RECONNECT_DELAY_MS = 30000;

async function start() {
  const { state, saveCreds } = await useMultiFileAuthState(config.sessionDir);
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
      console.log("Sur ton téléphone, DANS LES 60 SECONDES :");
      console.log("WhatsApp > Réglages > Appareils connectés");
      console.log("> Connecter un appareil > Connecter avec un numéro de téléphone");
      console.log("puis entre ce code.");
      console.log("");
      console.log(`(session sauvegardée dans : ${config.sessionDir})`);
    } catch (e) {
      console.error("❌ Impossible de générer le code :", e.message);
      process.exit(1);
    }
  }

  sock.ev.on("connection.update", ({ connection, lastDisconnect }) => {
    if (connection === "open") {
      console.log(`✅ ${config.botName} connecté à WhatsApp`);
      reconnectAttempts = 0; // connexion réussie, on repart à zéro
    }
    if (connection === "close") {
      const code = lastDisconnect?.error?.output?.statusCode;
      if (code === DisconnectReason.loggedOut) {
        console.log("❌ Session expirée ou invalide.");
        console.log(`   Supprime le dossier "${config.sessionDir}" puis relance pour générer un nouveau code de jumelage.`);
        if (config.sessionDir === "./session") {
          console.log("   💡 Si tu es sur Render (plan gratuit) : c'est normal, le disque est effacé à chaque redémarrage.");
          console.log("      Voir la note en haut du fichier pour une solution durable.");
        }
        return; // on NE relance PAS automatiquement : il faut un nouveau jumelage manuel
      }
      // Autre coupure (réseau, etc.) : reconnexion avec backoff progressif
      reconnectAttempts++;
      const delay = Math.min(1000 * 2 ** reconnectAttempts, MAX_RECONNECT_DELAY_MS);
      console.log(`🔄 Connexion perdue, nouvelle tentative dans ${Math.round(delay / 1000)}s...`);
      setTimeout(start, delay);
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
  // (démarré une seule fois, même si start() est rappelé après une reconnexion)
  if (!global.__nixWss) {
    const wss = new WebSocketServer({ port: config.wsPort });
    global.__nixWss = wss;
    console.log(`🔌 ${config.botName} — WebSocket en écoute sur ws://localhost:${config.wsPort}`);

    wss.on("connection", (ws) => {
      let id = null;

      ws.on("message", async (raw) => {
        let data;
        try { data = JSON.parse(raw); } catch { return; }

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

        if (data.type === "send" && data.to && data.text) {
          try {
            // on utilise toujours la socket WhatsApp la plus récente
            await global.__nixSock.sendMessage(data.to, { text: data.text });
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

  global.__nixSock = sock; // référence à jour pour le serveur WebSocket ci-dessus
}

start();
