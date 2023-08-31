const messageInput = document.querySelector("#message-input");
const sendButton = document.querySelector("#send-button");
const messageContainer = document.querySelector("#message-container");
const statusTemplate = document.querySelector(".status-template");
const opticSelector = document.querySelector("#optics-selector");

let prevState = null;

let originalSendButtonColor = sendButton.style.color;

let colorGreen = { r: 22, g: 163, b: 74, a: 1 };
let colorYellow = { r: 234, g: 179, b: 8, a: 1 };
let colorRed = { r: 220, g: 38, b: 38, a: 1 };

let scoreGradient = colorGradient(colorRed, colorYellow, 5).concat(colorGradient(colorYellow, colorGreen, 5));

let numMessageLines = 1;
let messageInputHeight = 24;
let firstMessage = true;
let messages = [];

let aliceResponding = false;
let aliceStreamingMessage = "";
let currentQueries = [];

// messages = [
//     {
//         "from": "user",
//         "message": "can i rent a car in santa cruz",
//         "queries": [],
//         "sourceColors": [],
//         "element": null
//     },
//     {
//         "from": "alice",
//         "message": " Yes, you can rent a car in Santa Cruz de la Sierra from $34 per day[query 1 source 1]. The car rental rates vary depending on the type of vehicle you choose, the duration of the rental, and any additional fees. You can also find cheap car rental deals in Santa Cruz de la Sierra from $24 - $35 per day[query 1 source 1]. Additionally, you can rent a car for a month for just $34[query 1 source 2]. It's important to note that driving in Santa Cruz de la Sierra can be challenging due to the country's challenging road conditions and drivers who may not follow traffic laws[query 1 source 3]. It's recommended that you hire a licensed driver and take extra precautions when driving in the city[query 1 source 3].",
//         "queries": [
//         {
//             "query": "rent car santa cruz",
//             "results": [
//             {
//                 "title": "Cheap Car Rental Deals in Santa Cruz de la Sierra from $34 - Cheapflights.com",
//                 "text": "month, costing you anywhere between $24 and $35 to fill up a car’s gas tank. Can I rent a car for a month in Santa Cruz de la Sierra? Yes,",
//                 "url": "https://www.cheapflights.com/car-rentals/santa-cruz/",
//                 "site": "cheapflights.com"
//             },
//             {
//                 "title": "What a Character Blogathon: How Arthur Kennedy Changed my Cinematic Life – The Wonderful World of Cinema",
//                 "text": "us smile, he’s just driving a car, but simply the fact that he’s here and that I was beginning to know him more and more. You know, just as if",
//                 "url": "https://thewonderfulworldofcinema.wordpress.com/2016/12/17/what-a-character-blogathon-how-arthur-kennedy-changed-my-cinematic-life/",
//                 "site": "thewonderfulworldofcinema.wordpress.com"
//             },
//             {
//                 "title": "Cop Killer Shouts Obscenities In Court, He Also Killed His Baby Mama… – Conversations Of A Sistah",
//                 "text": "likely. The devil didn’t make him steal his dads car, the devil didn’t make him kill those folks BUT now that he is up chit creek he is free to",
//                 "url": "https://conversationsofasistah.com/2012/01/31/cop-killer-shouts-obscenities-in-court-he-also-killed-his-baby-mama/",
//                 "site": "conversationsofasistah.com"
//             }
//             ]
//         }
//         ],
//         "sourceColors": [],
//         "element": null
//     }
// ];

// messageContainer.innerHTML = "";

// for (const message of messages) {
//     renderMessage(message);
// }

function colorGradient(start, end, steps) {
    const gradient = [];

    for (let i = 0; i < steps; i++) {
        const r = start.r + (end.r - start.r) * i / steps;
        const g = start.g + (end.g - start.g) * i / steps;
        const b = start.b + (end.b - start.b) * i / steps;
        const a = start.a + (end.a - start.a) * i / steps;
        gradient.push({ r, g, b, a });
    }

    return gradient;
}

function colorToString(color) {
    return `rgba(${color.r}, ${color.g}, ${color.b}, ${color.a})`;
}

function updateMessageInputHeight() {
    const lines = messageInput.value.split("\n");
    numMessageLines = lines.length;
    messageInput.style.height = `${messageInputHeight * numMessageLines}px`;
}

updateMessageInputHeight();

function scrollToBottom() {
    window.scrollTo(0, document.body.scrollHeight);
}

sendButton.addEventListener("click", sendMessage);

messageInput.addEventListener("keydown", (event) => {
    if (event.key === "Enter" && !event.shiftKey) {
        event.preventDefault();
        sendMessage().then(() => {
            updateMessageInputHeight();
        })
    }
});

messageInput.addEventListener("input", (_) => {
    updateMessageInputHeight();
});

async function sendMessage() {
    const message = messageInput.value;

    if (message.length == 0) {
        return;
    }

    if (firstMessage) {
        firstMessage = false;
        messageContainer.innerHTML = "";
    }

    addMessage("user", message);
    await sendMessageToAlice(message);
    messageInput.value = "";
}

async function savePrevState() {
    let url = "/beta/api/alice/save_state";
    var reqData = {
        'state': prevState,
    };

    // post request using fetch api
    let response = await fetch(url, {
        method: 'POST',
        body: JSON.stringify(reqData),
        headers: {
            'Content-Type': 'application/json'
        }
    });

    return await response.text();
}

function beginAliceStream() {
    aliceResponding = true;
    aliceStreamingMessage = "";

    // disable input
    messageInput.disabled = true;
    sendButton.style.color = "rgba(0, 0, 0, 0.25)";

    scrollToBottom();

    // show status
    const status = statusTemplate.cloneNode(true);
    status.classList.remove("status-template");
    status.classList.remove("hidden");

    status.id = "active-status"

    messageContainer.appendChild(status);
}

function streamMessage(data) {
    if (!aliceResponding) {
        return;
    }

    if (data["type"] == "speaking") {
        const status = document.querySelector("#active-status");

        if (status) {
            status.remove();
        }

        if (aliceStreamingMessage == "") {
            addMessage("alice", "", currentQueries);
            currentQueries = [];
        }
        aliceStreamingMessage += data["text"];

        const lastMessage = messages[messages.length - 1];
        lastMessage.message = aliceStreamingMessage;
        renderLastMessage();
    } else if (data["type"] == "beginSearch") {
        const status = document.querySelector("#active-status");
        status.querySelector("#info").innerHTML = `Looking up <span style="font-weight: bold;">${data["query"]}</span>`;
    } else if (data["type"] == "searchResult") {
        currentQueries.push({
            query: data["query"],
            results: data["result"]
        });
    } else if (data["type"] == "done") {
        prevState = data["state"];
        endAliceStream();
    }
}

function endAliceStream() {
    aliceResponding = false;
    aliceStreamingMessage = "";

    // enable input
    messageInput.disabled = false;
    sendButton.style.color = originalSendButtonColor;
    messageInput.focus();
}

async function sendMessageToAlice(message) {
    beginAliceStream();

    let chosenOptic = opticSelector.value;

    var reqData = {
        'message': message,
    };

    if (chosenOptic != "") {
        reqData['optic'] = chosenOptic;
    }

    if (prevState) {
        let savedState = await savePrevState();
        reqData['prevState'] = savedState;
    }

    var queryData = new URLSearchParams(reqData).toString();
    var source = new EventSource("/beta/api/alice?" + queryData);

    source.onmessage = function (event) {
        const data = JSON.parse(event.data);

        streamMessage(data);
    };

    // also called when the connection is closed from the server
    source.onerror = function (_) {
        endAliceStream();
        source.close();
    };
}

function addMessage(from, message, queries = []) {
    if (from != "user" && from != "alice") {
        throw new Error("Invalid message sender");
    }

    messages.push({
        from: from,
        message: message,
        queries: queries,
        sourceColors: [],
        element: null
    });

    renderLastMessage();
    scrollToBottom();
}

function renderLastMessage() {
    if (messages.length == 0) {
        return;
    }

    const message = messages[messages.length - 1];
    renderMessage(message);
}

function renderMessage(message) {
    if (message.element == null) {
        message.element = document.createElement("div");
        messageContainer.appendChild(message.element);
    }

    message.element.innerHTML = "";

    let renderedMessage = renderMarkdown(message.message);

    // replace [query i source j] with a link to the source
    // find using regex
    const regex = /\[query (\d+) source (\d+)\]/g;
    let match;
    let newRenderedMessage = renderedMessage;
    let messageSources = [];
    let nextSourceId = 1;

    let source2Id = {}; // (i, j) -> id

    let sourceIdx = 0;
    while ((match = regex.exec(renderedMessage)) !== null) {
        const queryNum = parseInt(match[1]);
        const sourceNum = parseInt(match[2]);

        let i = queryNum - 1;
        let j = sourceNum - 1;

        if (i >= message.queries.length) {
            newRenderedMessage = newRenderedMessage.replace(match[0], "");
            continue;
        }

        const query = message.queries[i];

        if (j >= query.results.length) {
            newRenderedMessage = newRenderedMessage.replace(match[0], "");
            continue;
        }
        const source = query.results[j];

        // find text from '.' to match[0]
        let claim = renderedMessage.substring(renderedMessage.lastIndexOf(".", match.index) + 1, match.index).trim();

        if (claim.startsWith("<p>")) {
            claim = claim.substring(3);
        }


        if (!(i in source2Id)) {
            source2Id[i] = {};
        }

        if (!(j in source2Id[i])) {
            source2Id[i][j] = nextSourceId;
            nextSourceId += 1;
        }

        // check if color is already assigned
        if (!(sourceIdx in message.sourceColors)) {
            factCheck(claim, source.text, sourceIdx).then((res) => {
                if (res != null) {
                    let [score, sourceIdx] = res;
                    // s is a score from 0 to 1
                    let colorId = Math.floor(score * (scoreGradient.length - 1));
                    let color = scoreGradient[colorId];


                    message.sourceColors[sourceIdx] = {
                        color: color,
                        score: score
                    };

                    updateSourceColors(message);
                }
            })
        }

        const id = source2Id[i][j];

        messageSources.push({
            query: query,
            source: source,
            claim: claim,
            id: id
        })

        const link = `<span class="inline-source inline-source-` + sourceIdx + `"><a href="` + source.url + `">` + id + `</a></span>`;

        newRenderedMessage = newRenderedMessage.replace(match[0], link);
        sourceIdx += 1;
    }

    // url and site of sources ordered by id
    let sourceUrls = [];

    for (let i = 0; i < nextSourceId; i++) {
        const source = messageSources.find(s => s.id == i + 1);

        if (source) {
            sourceUrls.push({
                url: source.source.url,
                site: source.source.site
            });
        }
    }

    renderedMessage = newRenderedMessage;

    if (message.from === "user") {
        message.element.classList.add("user-message-container");

        const msgElem = document.createElement("div");
        msgElem.classList.add("user-message-text");
        msgElem.innerHTML = renderedMessage;
        message.element.appendChild(msgElem);
    } else {
        message.element.classList.add("alice-message-container");

        const msgElem = document.createElement("div");
        msgElem.classList.add("alice-message");

        const msgText = document.createElement("div");
        msgText.classList.add("alice-message-text");
        msgText.innerHTML = renderedMessage;

        msgElem.appendChild(msgText);

        // add sources
        if (sourceUrls.length > 0) {
            const sources = document.createElement("div");
            sources.classList.add("message-sources");

            for (var i = 0; i < sourceUrls.length; i++) {
                let id = i + 1;
                const source = sourceUrls[i];

                const sourceLink = document.createElement("a");
                sourceLink.text = source.site;
                sourceLink.href = source.url;

                const sourceId = document.createElement("span");
                sourceId.classList.add("inline-source");
                sourceId.innerHTML = `<a href="` + source.url + `">` + id.toString() + `</a>`;

                const sourceContainer = document.createElement("div");
                sourceContainer.classList.add("message-source");
                sourceContainer.appendChild(sourceId);
                sourceContainer.appendChild(sourceLink);

                sources.appendChild(sourceContainer);
            }

            msgElem.appendChild(sources);
        }

        message.element.appendChild(msgElem);
    }

    updateSourceColors(message);

    // @ts-ignore
    hljs.highlightAll();
    removeBackgroundFromCode();
}

async function factCheck(claim, evidence, sourceIdx) {
    let url = "/beta/api/fact_check";

    const response = await fetch(url, {
        method: "POST",
        headers: {
            "Content-Type": "application/json"
        },
        body: JSON.stringify({
            claim: claim,
            evidence: evidence
        })
    });

    if (response.status == 200) {
        const data = await response.json();
        return [data.score, sourceIdx];
    } else {
        return null;
    }
}

function updateSourceColors(message) {
    if (message.element == null) {
        return;
    }

    // loop through the sourceColors
    for (const idx in message.sourceColors) {
        const scoredColor = message.sourceColors[idx];
        const source = message.element.querySelector(".inline-source-" + idx);

        if (source) {
            source.style.borderColor = colorToString(scoredColor.color);
            source.title = Math.floor(scoredColor.score * 100) + "% confidence that source supports claim";
        }
    }

}

function renderMarkdown(markdown) {
    // @ts-ignore
    const html = marked.parse(markdown);
    // @ts-ignore
    const sanitized = DOMPurify.sanitize(html);
    return sanitized;
}

function removeBackgroundFromCode() {
    const codeBlocks = document.querySelectorAll("code");
    for (const codeBlock of codeBlocks) {
        codeBlock.style.background = "none";
    }
}