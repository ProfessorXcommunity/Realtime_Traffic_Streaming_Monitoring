const axios = require("axios");

function randomEvent() {
  return {
    user_id: "user_" + Math.floor(Math.random() * 1000),
    page: ["/", "/login", "/checkout"][Math.floor(Math.random() * 3)],
    event_type: "page_view",
    response_time_ms: Math.floor(Math.random() * 500),
    status_code: Math.random() < 0.9 ? 200 : 500,
  };
}

setInterval(async () => {
  try {
    await axios.post("http://localhost:8000/event", randomEvent());
    console.log("event sent");
  } catch (e) {
    console.log("Error sending event");
  }
}, 500);
