(() => {
  "use strict";

  const webApp = window.Telegram && window.Telegram.WebApp;
  const status = document.getElementById("status");
  const identity = document.getElementById("identity");
  const telegramId = document.getElementById("telegram-id");
  const usernameRow = document.getElementById("username-row");
  const username = document.getElementById("username");

  const deny = () => {
    status.textContent = "Мини-приложение пока доступно только администраторам.";
    identity.hidden = true;
  };

  if (!webApp || !webApp.initData) {
    deny();
    return;
  }

  webApp.ready();
  webApp.expand();

  let sessionToken = null;

  fetch("/api/admin/session", {
    method: "POST",
    headers: {Authorization: `tma ${webApp.initData}`},
    cache: "no-store",
    credentials: "omit",
  })
    .then((response) => {
      if (!response.ok) throw new Error("telegram_session_expired");
      return response.json();
    })
    .then((session) => {
      sessionToken = session.token;
      return fetch("/api/admin/me", {
        method: "GET",
        headers: {Authorization: `Bearer ${sessionToken}`},
        cache: "no-store",
        credentials: "omit",
      });
    })
    .then((response) => {
      if (!response.ok) throw new Error("access_denied");
      return response.json();
    })
    .then((data) => {
      status.textContent = "Доступ подтверждён";
      telegramId.textContent = String(data.telegram_id);
      identity.hidden = false;
      if (data.username) {
        username.textContent = `@${data.username}`;
        usernameRow.hidden = false;
      }
    })
    .catch((error) => {
      if (error.message === "telegram_session_expired") {
        status.textContent = "Сессия Telegram устарела. Закройте и откройте мини-приложение снова.";
        identity.hidden = true;
        return;
      }
      deny();
    });
})();
