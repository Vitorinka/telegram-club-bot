(() => {
  "use strict";

  const webApp = window.Telegram && window.Telegram.WebApp;
  const status = document.getElementById("status");
  const identity = document.getElementById("identity");
  const telegramId = document.getElementById("telegram-id");
  const usernameRow = document.getElementById("username-row");
  const username = document.getElementById("username");
  const dashboard = document.getElementById("dashboard");
  const refresh = document.getElementById("refresh");
  const metricNodes = document.querySelectorAll("[data-metric]");

  const deny = () => {
    status.textContent = "Мини-приложение пока доступно только администраторам.";
    identity.hidden = true;
    dashboard.hidden = true;
    refresh.hidden = true;
  };

  const valueAtPath = (object, path) => path.split(".").reduce(
    (value, key) => value && value[key], object
  );

  const renderDashboard = (data) => {
    metricNodes.forEach((node) => {
      const value = valueAtPath(data, node.dataset.metric);
      node.textContent = value === null || value === undefined ? "—" : String(value);
    });
    dashboard.hidden = false;
    refresh.hidden = false;
    status.textContent = "Доступ подтверждён";
  };

  if (!webApp || !webApp.initData) {
    deny();
    return;
  }

  webApp.ready();
  webApp.expand();

  let sessionToken = null;

  const loadDashboard = () => {
    status.textContent = "Загружаем данные…";
    return fetch("/api/admin/dashboard", {
      method: "GET",
      headers: {Authorization: `Bearer ${sessionToken}`},
      cache: "no-store",
      credentials: "omit",
    }).then((response) => {
      if (response.status === 401) throw new Error("session_ended");
      if (response.status === 403) throw new Error("access_revoked");
      if (!response.ok) throw new Error("dashboard_failed");
      return response.json();
    }).then(renderDashboard);
  };

  refresh.addEventListener("click", () => {
    loadDashboard().catch(showApiError);
  });

  function showApiError(error) {
    dashboard.hidden = true;
    refresh.hidden = true;
    if (error.message === "session_ended") {
      status.textContent = "Сессия завершена. Закройте и снова откройте админ-платформу.";
    } else if (error.message === "access_revoked") {
      status.textContent = "У вас больше нет доступа к админ-платформе.";
    } else {
      status.textContent = "Не удалось загрузить данные. Попробуйте обновить.";
      refresh.hidden = false;
    }
  }

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
      return loadDashboard();
    })
    .catch((error) => {
      if (error.message === "telegram_session_expired") {
        status.textContent = "Сессия Telegram устарела. Закройте и откройте мини-приложение снова.";
        identity.hidden = true;
        return;
      }
      showApiError(error);
    });
})();
