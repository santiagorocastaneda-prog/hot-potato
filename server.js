const express = require("express");
const http = require("node:http");
const { randomBytes, randomInt } = require("node:crypto");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);
const io = new Server(server);
const rooms = new Map();

function normalize(value) {
return value.trim().toLowerCase()
.normalize("NFD").replace(/[\u0300-\u036f]/g, "")
.replace(/[^a-z0-9]/g, "");
}

const countryNames = `
Afghanistan|Albania|Algeria|Andorra|Angola|Antigua and Barbuda|
Argentina|Armenia|Australia|Austria|Azerbaijan|Bahamas|Bahrain|
Bangladesh|Barbados|Belarus|Belgium|Belize|Benin|Bhutan|Bolivia|
Bosnia and Herzegovina|Botswana|Brazil|Brunei|Bulgaria|Burkina Faso|
Burundi|Cabo Verde|Cambodia|Cameroon|Canada|Central African Republic|
Chad|Chile|China|Colombia|Comoros|Republic of the Congo|
Democratic Republic of the Congo|Costa Rica|Croatia|Cuba|Cyprus|
Czechia|Denmark|Djibouti|Dominica|Dominican Republic|Ecuador|Egypt|
El Salvador|Equatorial Guinea|Eritrea|Estonia|Eswatini|Ethiopia|
Fiji|Finland|France|Gabon|Gambia|Georgia|Germany|Ghana|Greece|
Grenada|Guatemala|Guinea|Guinea-Bissau|Guyana|Haiti|Honduras|
Hungary|Iceland|India|Indonesia|Iran|Iraq|Ireland|Israel|Italy|
Ivory Coast|Jamaica|Japan|Jordan|Kazakhstan|Kenya|Kiribati|
Kuwait|Kyrgyzstan|Laos|Latvia|Lebanon|Lesotho|Liberia|Libya|
Liechtenstein|Lithuania|Luxembourg|Madagascar|Malawi|Malaysia|
Maldives|Mali|Malta|Marshall Islands|Mauritania|Mauritius|Mexico|
Micronesia|Moldova|Monaco|Mongolia|Montenegro|Morocco|Mozambique|
Myanmar|Namibia|Nauru|Nepal|Netherlands|New Zealand|Nicaragua|
Niger|Nigeria|North Korea|North Macedonia|Norway|Oman|Pakistan|
Palau|Palestine|Panama|Papua New Guinea|Paraguay|Peru|Philippines|
Poland|Portugal|Qatar|Romania|Russia|Rwanda|Saint Kitts and Nevis|
Saint Lucia|Saint Vincent and the Grenadines|Samoa|San Marino|
Sao Tome and Principe|Saudi Arabia|Senegal|Serbia|Seychelles|
Sierra Leone|Singapore|Slovakia|Slovenia|Solomon Islands|Somalia|
South Africa|South Korea|South Sudan|Spain|Sri Lanka|Sudan|
Suriname|Sweden|Switzerland|Syria|Tajikistan|Tanzania|Thailand|
Timor-Leste|Togo|Tonga|Trinidad and Tobago|Tunisia|Turkey|
Turkmenistan|Tuvalu|Uganda|Ukraine|United Arab Emirates|
United Kingdom|United States|Uruguay|Uzbekistan|Vanuatu|
Vatican City|Venezuela|Vietnam|Yemen|Zambia|Zimbabwe
`;

const countries = new Map();
countryNames.split("|").forEach((name) => {
name = name.trim();
countries.set(normalize(name), name);
});

const aliases = {
usa: "United States",
us: "United States",
unitedstatesofamerica: "United States",
uk: "United Kingdom",
britain: "United Kingdom",
greatbritain: "United Kingdom",
uae: "United Arab Emirates",
czechrepublic: "Czechia",
"cape verde": "Cabo Verde",
turkiye: "Turkey",
burma: "Myanmar",
swaziland: "Eswatini",
easttimor: "Timor-Leste",
drc: "Democratic Republic of the Congo",
congo: "Republic of the Congo",
"cote divoire": "Ivory Coast",
};

Object.entries(aliases).forEach(([alias, name]) => {
countries.set(normalize(alias), name);
});

function aliveIds(room) {
return [...room.players.keys()].filter(
(id) => !room.players.get(id).out
);
}

function broadcast(code) {
const room = rooms.get(code);
if (!room) return;

io.to(code).emit("state", {
code,
host: room.host,
players: [...room.players].map(([id, player]) => ({
id, ...player,
})),
phase: room.phase,
holder: room.holder,
deadline: room.deadline,
serverNow: Date.now(),
message: room.message,
used: [...room.used],
});
}

function finish(code, message) {
const room = rooms.get(code);
if (!room) return;

clearTimeout(room.timer);
room.timer = null;
room.phase = "finished";
room.holder = null;
room.deadline = 0;
room.message = message;
broadcast(code);
}

function beginTurn(code, holder) {
const room = rooms.get(code);
if (!room) return;

clearTimeout(room.timer);
const alive = aliveIds(room);

if (alive.length < 2) {
return finish(code, alive.length
? room.players.get(alive[0]).name + " wins!"
: "Match ended.");
}

room.phase = "playing";
room.holder = holder || alive[randomInt(alive.length)];
room.deadline = Date.now() + 8000;
room.timer = setTimeout(() => timeUp(code), 8000);
broadcast(code);
}

function timeUp(code) {
const room = rooms.get(code);
if (!room || room.phase !== "playing") return;

const remaining = room.deadline - Date.now();
if (remaining > 0) {
clearTimeout(room.timer);
room.timer = setTimeout(() => timeUp(code), remaining);
return;
}

clearTimeout(room.timer);
const loser = room.players.get(room.holder);
loser.out = true;
room.holder = null;
room.deadline = 0;

const alive = aliveIds(room);
if (alive.length < 2) {
return finish(code,
loser.name + " is out! "
+ (alive.length ? room.players.get(alive[0]).name + " wins!" : ""));
}

room.phase = "between";
room.message = loser.name + " is out! Next turn in 3 seconds.";
room.timer = setTimeout(() => {
room.message = "Name an unused country!";
beginTurn(code);
}, 3000);
broadcast(code);
}

function leaveRoom(socket) {
const code = socket.data.roomCode;
if (!code) return;

socket.leave(code);
socket.data.roomCode = null;
const room = rooms.get(code);
if (!room) return;

room.players.delete(socket.id);
if (!room.players.size) {
clearTimeout(room.timer);
rooms.delete(code);
return;
}

if (room.host === socket.id) {
room.host = room.players.keys().next().value;
}

if (room.phase === "playing" || room.phase === "between") {
finish(code, "A player left. The host can start a new match.");
} else {
broadcast(code);
}
}

io.on("connection", (socket) => {
const fail = (message) => socket.emit("roomError", message);

socket.on("createRoom", (name) => {
if (typeof name !== "string" || !name.trim()) {
return fail("Enter your name.");
}

leaveRoom(socket);
let code;
do {
code = randomBytes(3).toString("hex").toUpperCase();
} while (rooms.has(code));

rooms.set(code, {
host: socket.id,
players: new Map([[socket.id, {
name: name.trim().slice(0, 20), out: false,
}]]),
phase: "lobby",
holder: null,
deadline: 0,
timer: null,
used: new Set(),
message: "Countries mode — waiting for players.",
});

socket.data.roomCode = code;
socket.join(code);
broadcast(code);
});

socket.on("joinRoom", (data) => {
if (!data || typeof data.name !== "string" ||
!data.name.trim() || typeof data.code !== "string") {
return fail("Enter your name and room code.");
}

const code = data.code.trim().toUpperCase();
const room = rooms.get(code);
if (!room) return fail("Room not found.");
if (socket.data.roomCode === code) return broadcast(code);
if (room.phase === "playing" || room.phase === "between") {
return fail("Wait until the match ends.");
}

leaveRoom(socket);
room.players.set(socket.id, {
name: data.name.trim().slice(0, 20), out: false,
});
socket.data.roomCode = code;
socket.join(code);
broadcast(code);
});

socket.on("startMatch", () => {
const code = socket.data.roomCode;
const room = rooms.get(code);
if (!room) return fail("Join a room first.");
if (room.host !== socket.id) return fail("Only the host can start.");
if (room.phase === "playing" || room.phase === "between") return;
if (room.players.size < 2) return fail("You need at least two players.");

room.players.forEach((player) => { player.out = false; });
room.used.clear();
room.message = "Name an unused country!";
beginTurn(code);
});

socket.on("submitAnswer", (answer) => {
const code = socket.data.roomCode;
const room = rooms.get(code);
if (!room || room.phase !== "playing") return;
if (Date.now() >= room.deadline) return timeUp(code);
if (room.holder !== socket.id) return fail("Wait for your turn.");
if (typeof answer !== "string" || answer.length > 80) {
return fail("Enter a country name.");
}

const country = countries.get(normalize(answer));
if (!country) return fail("Country not recognized. Try its English name!");
if (room.used.has(country)) return fail("Already used! Try another country.");

room.used.add(country);
const alive = aliveIds(room);
const next = alive[(alive.indexOf(socket.id) + 1) % alive.length];
room.message = room.players.get(socket.id).name
+ " answered " + country + "!";
beginTurn(code, next);
});

socket.on("disconnect", () => leaveRoom(socket));
});

app.get("/", (req, res) => {
res.send(`
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Hot Potato — Countries</title>
<style>
:root {
color-scheme: dark;
font-family: system-ui, sans-serif;
background: #10111b;
color: #f4f4fa;
}
* { box-sizing: border-box; }
body {
width: min(100% - 32px, 580px);
margin: 30px auto;
padding: 24px;
background: #1b1d2c;
border: 1px solid #34374c;
border-radius: 24px;
}
h1 { font-size: 36px; margin-top: 0; }
input, button {
width: 100%;
min-height: 52px;
margin: 8px 0;
padding: 14px;
border-radius: 12px;
font: inherit;
font-size: 18px;
}
input {
background: #111320;
border: 1px solid #454962;
color: white;
}
button {
background: #ffc078;
color: #241303;
border: 0;
font-weight: 750;
cursor: pointer;
touch-action: manipulation;
}
button:disabled { background: #303348; color: #b0b5cd; }
:focus-visible { outline: 3px solid #c4b5fd; outline-offset: 3px; }
#players { list-style: none; padding: 0; }
#players li {
padding: 14px;
margin: 8px 0;
background: #25283b;
border-radius: 12px;
overflow-wrap: anywhere;
}
#message { color: #ffd29c; overflow-wrap: anywhere; }
#clock { font-size: 34px; font-weight: bold; color: #c4b5fd; }
#error { color: #ffb6c4; }
#used, #status, .hint { color: #b0b5cd; line-height: 1.5; }
#room, #used { overflow-wrap: anywhere; }
#error:empty { display: none; }
</style>
</head>
<body>
<h1>🥔 Hot Potato</h1>
<p class="hint">Countries mode: answer in English. No repeats.
You have 8 seconds each turn!</p>
<p id="status">Connecting...</p>

<input id="name" aria-label="Your name" placeholder="Your name" maxlength="20">
<button id="create" disabled>Create Room</button>
<input id="code" aria-label="Room code" placeholder="Room code" maxlength="6">
<button id="join" disabled>Join Room</button>

<h2 id="room">No room joined</h2>
<ul id="players"></ul>
<h2 id="message" aria-live="polite"></h2>
<p id="clock">—</p>

<form id="answerForm">
<input id="answer" aria-label="Country answer"
placeholder="Type a country..." maxlength="80"
autocomplete="off" disabled>
<button id="submit" disabled>Submit Country</button>
</form>

<p id="error" role="alert"></p>
<button id="start" disabled>Start Match</button>
<p id="used"></p>

<script src="/socket.io/socket.io.js"></script>
<script>
const socket = io();
const el = (id) => document.getElementById(id);
let currentState = null;
let clockOffset = 0;

socket.on("connect", () => {
el("status").textContent = "Connected!";
el("create").disabled = false;
el("join").disabled = false;
});

socket.on("disconnect", () => {
currentState = null;
el("status").textContent = "Disconnected. Rejoin after reconnecting.";
["create", "join", "start", "answer", "submit"].forEach((id) => {
el(id).disabled = true;
});
el("room").textContent = "No room joined";
el("players").replaceChildren();
el("message").textContent = "";
el("used").textContent = "";
el("error").textContent = "";
el("clock").textContent = "—";
});

socket.on("connect_error", () => {
el("status").textContent = "Connection failed. Check the server.";
});

el("create").onclick = () => {
el("error").textContent = "";
socket.emit("createRoom", el("name").value);
};

el("join").onclick = () => {
el("error").textContent = "";
socket.emit("joinRoom", {
name: el("name").value,
code: el("code").value
});
};

el("start").onclick = () => socket.emit("startMatch");

el("answerForm").onsubmit = (event) => {
event.preventDefault();
if (el("submit").disabled) return;
el("error").textContent = "";
socket.emit("submitAnswer", el("answer").value);
};

socket.on("roomError", (message) => {
el("error").textContent = message;
});

socket.on("state", (state) => {
const wasMyTurn = currentState
&& currentState.phase === "playing"
&& currentState.holder === socket.id;
currentState = state;
clockOffset = state.serverNow - Date.now();

el("error").textContent = "";
el("room").textContent = "Room code: " + state.code;
el("players").replaceChildren();

state.players.forEach((player) => {
const item = document.createElement("li");
item.textContent = player.name
+ (player.id === socket.id ? " (you)" : "")
+ (player.id === state.host ? " — host" : "")
+ (player.out ? " — OUT" : "")
+ (player.id === state.holder ? " — 🥔 YOUR TURN" : "");
el("players").appendChild(item);
});

el("message").textContent = state.message;
el("used").textContent = "Used countries: "
+ (state.used.join(", ") || "None yet");

const active = state.phase === "playing" || state.phase === "between";
const myTurn = state.phase === "playing" && state.holder === socket.id;

el("start").disabled = active || state.host !== socket.id
|| state.players.length < 2;
el("answer").disabled = !myTurn;
el("submit").disabled = !myTurn;

if (!myTurn || !wasMyTurn) el("answer").value = "";
if (myTurn && !wasMyTurn) el("answer").focus();
updateClock();
});

function updateClock() {
if (!currentState || currentState.phase !== "playing") {
el("clock").textContent = "—";
return;
}
const remaining = Math.max(0,
currentState.deadline - (Date.now() + clockOffset));
el("clock").textContent = (remaining / 1000).toFixed(1) + "s";
}

setInterval(updateClock, 100);
</script>
</body>
</html>
`);
});

const port = process.env.PORT || 3000;
server.listen(port, () => {
console.log("Server running on " + port);
});
