(() => {
  "use strict";
  const webApp = window.Telegram && window.Telegram.WebApp;
  const status = document.getElementById("status");
  const identity = document.getElementById("identity");
  const telegramId = document.getElementById("telegram-id");
  const refresh = document.getElementById("refresh");
  const bottomNav = document.getElementById("bottom-nav");
  const usersSearch = document.getElementById("users-search");
  const usersStatus = document.getElementById("users-status");
  const usersList = document.getElementById("users-list");
  const usersMore = document.getElementById("users-more");
  const detailsContent = document.getElementById("user-details-content");
  const metricNodes = document.querySelectorAll("[data-metric]");
  const statusLabels = {active: "Активен", active_grace: "Grace", expired: "Просрочен", inactive: "Нет доступа"};
  const typeLabels = {trial: "Trial", paid: "Платная", gift: "Подарок", manual: "Ручной", unknown: "Не определено"};
  let sessionToken = null;
  let usersCursor = null;
  let searchTimer = null;

  const text = (tag, value, className) => {
    const node = document.createElement(tag);
    node.textContent = value === null || value === undefined || value === "" ? "—" : String(value);
    if (className) node.className = className;
    return node;
  };
  const api = (path) => fetch(path, {
    method: "GET", headers: {Authorization: `Bearer ${sessionToken}`},
    cache: "no-store", credentials: "omit",
  }).then((response) => {
    if (response.status === 401) throw new Error("session_ended");
    if (response.status === 403) throw new Error("access_revoked");
    if (!response.ok) throw new Error("api_failed");
    return response.json();
  });
  const showApiError = (error) => {
    if (error.message === "session_ended") status.textContent = "Сессия завершена. Закройте и снова откройте админ-платформу.";
    else if (error.message === "access_revoked") status.textContent = "У вас больше нет доступа к админ-платформе.";
    else status.textContent = "Не удалось загрузить данные. Попробуйте обновить.";
  };
  const showScreen = (name) => {
    document.querySelectorAll("[data-screen]").forEach((node) => { node.hidden = node.dataset.screen !== name; });
    document.querySelectorAll("[data-nav]").forEach((node) => { node.classList.toggle("active", node.dataset.nav === name); });
  };
  const valueAtPath = (object, path) => path.split(".").reduce((value, key) => value && value[key], object);
  const loadDashboard = () => {
    status.textContent = "Загружаем данные…";
    return api("/api/admin/dashboard").then((data) => {
      metricNodes.forEach((node) => { node.textContent = String(valueAtPath(data, node.dataset.metric) ?? "—"); });
      showScreen("overview");
      refresh.hidden = false;
      status.textContent = "Доступ подтверждён";
    });
  };
  const addBadges = (container, user) => {
    const badges = document.createElement("div");
    badges.className = "badges";
    badges.append(text("span", statusLabels[user.access_status] || user.access_status, "badge"));
    badges.append(text("span", typeLabels[user.access_type] || user.access_type, "badge"));
    if (user.auto_renew) badges.append(text("span", "Автопродление", "badge"));
    if (user.payment_failed) badges.append(text("span", "Ошибка оплаты", "badge"));
    container.append(badges);
  };
  const userCard = (user) => {
    const article = document.createElement("article");
    article.className = "card user-card";
    const button = document.createElement("button");
    button.type = "button";
    button.append(text("h2", user.username ? `@${user.username}` : "Без username"));
    button.append(text("p", `Telegram ID: ${user.telegram_id}`));
    button.append(text("p", `Доступ до: ${user.expiry_date || "—"}`));
    addBadges(button, user);
    button.addEventListener("click", () => loadUserDetails(user.telegram_id));
    article.append(button);
    return article;
  };
  const loadUsers = (append = false) => {
    status.textContent = "Загружаем пользователей…";
    const params = new URLSearchParams({limit: "25", status: usersStatus.value});
    if (usersSearch.value.trim()) params.set("q", usersSearch.value.trim());
    if (append && usersCursor) params.set("cursor", usersCursor);
    return api(`/api/admin/users?${params.toString()}`).then((data) => {
      if (!append) usersList.replaceChildren();
      data.items.forEach((user) => usersList.append(userCard(user)));
      usersCursor = data.next_cursor;
      usersMore.hidden = !data.has_more;
      showScreen("users");
      status.textContent = `Пользователей показано: ${usersList.children.length}`;
    });
  };
  const detailCard = (title, pairs) => {
    const article = document.createElement("article");
    article.className = "card";
    article.append(text("h2", title));
    const list = document.createElement("dl");
    pairs.forEach(([label, value]) => {
      const row = document.createElement("div");
      row.append(text("dt", label), text("dd", value));
      list.append(row);
    });
    article.append(list);
    return article;
  };
  function loadUserDetails(userId) {
    status.textContent = "Загружаем профиль…";
    return api(`/api/admin/users/${encodeURIComponent(userId)}`).then((user) => {
      detailsContent.replaceChildren();
      detailsContent.append(
        detailCard("Профиль", [["Telegram ID", user.telegram_id], ["Username", user.username ? `@${user.username}` : "—"], ["Имя", [user.first_name, user.last_name].filter(Boolean).join(" ") || "—"]]),
        detailCard("Доступ", [["Статус", statusLabels[user.access_status]], ["Тип", typeLabels[user.access_type]], ["До", user.expiry_date], ["Trial использован", user.trial_used ? "Да" : "Нет"]]),
        detailCard("Оплата", [["Paid", user.paid ? "Да" : "Нет"], ["Автопродление", user.auto_renew ? "Да" : "Нет"], ["Ошибка оплаты", user.payment_failed ? "Да" : "Нет"], ["Grace до", user.grace_period_end], ["Customer", user.stripe.customer_id], ["Subscription", user.stripe.subscription_id]]),
        detailCard("Удаление", user.removal ? [["Статус", user.removal.status], ["Причина", user.removal.reason], ["Access expiry", user.removal.access_expiry], ["Обновлено", user.removal.updated_at]] : [["Статус", "Нет операции"]]),
        detailCard("История", user.access_history.length ? user.access_history.map((event) => [event.event_type, `${event.source}: ${event.old_expiry || "—"} → ${event.new_expiry || "—"}`]) : [["События", "Нет"]])
      );
      showScreen("user-details");
      status.textContent = "Профиль пользователя";
    }).catch(showApiError);
  }

  if (!webApp || !webApp.initData) {
    status.textContent = "Мини-приложение пока доступно только администраторам.";
    identity.hidden = true;
    return;
  }
  webApp.ready();
  webApp.expand();
  document.querySelectorAll("[data-nav]").forEach((button) => {
    button.addEventListener("click", () => {
      if (button.dataset.nav === "overview") loadDashboard().catch(showApiError);
      else if (button.dataset.nav === "users") loadUsers().catch(showApiError);
      else showScreen(button.dataset.nav);
    });
  });
  refresh.addEventListener("click", () => loadDashboard().catch(showApiError));
  usersMore.addEventListener("click", () => loadUsers(true).catch(showApiError));
  usersStatus.addEventListener("change", () => loadUsers().catch(showApiError));
  usersSearch.addEventListener("input", () => {
    window.clearTimeout(searchTimer);
    searchTimer = window.setTimeout(() => loadUsers().catch(showApiError), 300);
  });
  document.getElementById("users-back").addEventListener("click", () => showScreen("users"));
  fetch("/api/admin/session", {
    method: "POST", headers: {Authorization: `tma ${webApp.initData}`},
    cache: "no-store", credentials: "omit",
  }).then((response) => {
    if (!response.ok) throw new Error("telegram_session_expired");
    return response.json();
  }).then((session) => {
    sessionToken = session.token;
    return api("/api/admin/me");
  }).then((admin) => {
    telegramId.textContent = String(admin.telegram_id);
    identity.hidden = false;
    bottomNav.hidden = false;
    return loadDashboard();
  }).catch((error) => {
    if (error.message === "telegram_session_expired") {
      status.textContent = "Сессия Telegram устарела. Закройте и откройте мини-приложение снова.";
      identity.hidden = true;
      return;
    }
    showApiError(error);
  });
})();
