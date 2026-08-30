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
  const manualAccessCard = document.getElementById("manual-access-card");
  const manualAccessControls = document.getElementById("manual-access-controls");
  const manualAccessDuration = document.getElementById("manual-access-duration");
  const manualAccessConfirmation = document.getElementById("manual-access-confirmation");
  const manualAccessSummary = document.getElementById("manual-access-summary");
  const manualAccessWarnings = document.getElementById("manual-access-warnings");
  const manualAccessMessage = document.getElementById("manual-access-message");
  const manualAccessConfirm = document.getElementById("manual-access-confirm");
  const manualAccessCancel = document.getElementById("manual-access-cancel");
  const subscriptionsSearch = document.getElementById("subscriptions-search");
  const subscriptionsState = document.getElementById("subscriptions-state");
  const subscriptionsList = document.getElementById("subscriptions-list");
  const subscriptionsMore = document.getElementById("subscriptions-more");
  const subscriptionDetailsContent = document.getElementById("subscription-details-content");
  const subscriptionMetricNodes = document.querySelectorAll("[data-subscription-metric]");
  const systemMetricNodes = document.querySelectorAll("[data-system-metric]");
  const systemAttention = document.getElementById("system-attention");
  const systemDeliveryMetrics = document.getElementById("system-delivery-metrics");
  const systemMigrations = document.getElementById("system-migrations");
  const schedulerRuns = document.getElementById("scheduler-runs");
  const deliveriesStatus = document.getElementById("deliveries-status");
  const deliveriesList = document.getElementById("deliveries-list");
  const deliveriesMore = document.getElementById("deliveries-more");
  const deliveryDetailsContent = document.getElementById("delivery-details-content");
  const scheduleList = document.getElementById("schedule-list");
  const scheduleHeading = document.getElementById("schedule-heading");
  const scheduleEmpty = document.getElementById("schedule-empty");
  const scheduleMore = document.getElementById("schedule-more");
  const scheduleDetailsContent = document.getElementById("schedule-details-content");
  const scheduleMetricNodes = document.querySelectorAll("[data-schedule-metric]");
  const scheduleUploadMonth = document.getElementById("schedule-upload-month");
  const scheduleUploadFile = document.getElementById("schedule-upload-file");
  const scheduleUploadPreview = document.getElementById("schedule-upload-preview");
  const scheduleUploadMessage = document.getElementById("schedule-upload-message");
  const scheduleUploadConfirm = document.getElementById("schedule-upload-confirm");
  const giftsSearch = document.getElementById("gifts-search");
  const giftsStatus = document.getElementById("gifts-status");
  const giftsDuration = document.getElementById("gifts-duration");
  const giftsList = document.getElementById("gifts-list");
  const giftsEmpty = document.getElementById("gifts-empty");
  const giftsMore = document.getElementById("gifts-more");
  const giftDetailsContent = document.getElementById("gift-details-content");
  const giftResendCard = document.getElementById("gift-resend-card");
  const giftResendUnavailable = document.getElementById("gift-resend-unavailable");
  const giftResendControls = document.getElementById("gift-resend-controls");
  const giftResendTarget = document.getElementById("gift-resend-target");
  const giftResendConfirmation = document.getElementById("gift-resend-confirmation");
  const giftResendSummary = document.getElementById("gift-resend-summary");
  const giftResendMessage = document.getElementById("gift-resend-message");
  const giftResendConfirm = document.getElementById("gift-resend-confirm");
  const giftResendCancel = document.getElementById("gift-resend-cancel");
  const giftMetricNodes = document.querySelectorAll("[data-gift-metric]");
  const contentSearch = document.getElementById("content-search");
  const contentCategory = document.getElementById("content-category");
  const contentList = document.getElementById("content-list");
  const contentEmpty = document.getElementById("content-empty");
  const contentDetailsContent = document.getElementById("content-details-content");
  const cmsContentList = document.getElementById("cms-content-list");
  const cmsContentEmpty = document.getElementById("cms-content-empty");
  const contentCreateTitle = document.getElementById("content-create-title");
  const contentCreateCategory = document.getElementById("content-create-category");
  const contentCreateDescription = document.getElementById("content-create-description");
  const contentCreateDuration = document.getElementById("content-create-duration");
  const contentCreateMessage = document.getElementById("content-create-message");
  const contentEditCard = document.getElementById("content-edit-card");
  const contentEditTitle = document.getElementById("content-edit-title");
  const contentEditCategory = document.getElementById("content-edit-category");
  const contentEditDescription = document.getElementById("content-edit-description");
  const contentEditDuration = document.getElementById("content-edit-duration");
  const contentEditOrder = document.getElementById("content-edit-order");
  const contentEditMessage = document.getElementById("content-edit-message");
  const contentMediaCard = document.getElementById("content-media-card");
  const contentCoverCurrent = document.getElementById("content-cover-current");
  const contentVideoCurrent = document.getElementById("content-video-current");
  const contentCoverFile = document.getElementById("content-cover-file");
  const contentVideoFile = document.getElementById("content-video-file");
  const contentMediaConfirmation = document.getElementById("content-media-confirmation");
  const contentMediaPreview = document.getElementById("content-media-preview");
  const contentMediaSummary = document.getElementById("content-media-summary");
  const contentMediaMessage = document.getElementById("content-media-message");
  const contentMediaConfirm = document.getElementById("content-media-confirm");
  const contentMediaCancel = document.getElementById("content-media-cancel");
  const metricNodes = document.querySelectorAll("[data-metric]");
  const statusLabels = {active: "Активен", active_grace: "Grace", expired: "Просрочен", inactive: "Нет доступа"};
  const typeLabels = {trial: "Trial", paid: "Платная", gift: "Подарок", manual: "Ручной", unknown: "Не определено"};
  let sessionToken = null;
  let usersCursor = null;
  let searchTimer = null;
  let subscriptionsCursor = null;
  let subscriptionsSearchTimer = null;
  let deliveriesCursor = null;
  let scheduleCursor = null;
  let scheduleRange = "future";
  let scheduleImageGeneration = 0;
  const scheduleImageUrls = new Map();
  let scheduleUploadLocalUrl = null;
  let scheduleUploadServerUrl = null;
  let scheduleUploadId = null;
  let giftsCursor = null;
  let giftsSearchTimer = null;
  let giftResendGiftId = null;
  let giftResendActionId = null;
  let manualAccessUserId = null;
  let manualAccessActionId = null;
  let contentSearchTimer = null;
  let currentCmsContent = null;
  let contentMediaUploadId = null;
  let contentMediaLocalUrl = null;
  let contentMediaServerUrl = null;
  let contentMediaAttachedCoverUrl = null;

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
  const clearScheduleImages = () => {
    scheduleImageGeneration += 1;
    scheduleImageUrls.forEach((url) => URL.revokeObjectURL(url));
    scheduleImageUrls.clear();
  };
  const clearScheduleUploadUrls = () => {
    if (scheduleUploadLocalUrl) URL.revokeObjectURL(scheduleUploadLocalUrl);
    if (scheduleUploadServerUrl) URL.revokeObjectURL(scheduleUploadServerUrl);
    scheduleUploadLocalUrl = null;
    scheduleUploadServerUrl = null;
  };
  const clearContentMediaUrls = () => {
    if (contentMediaLocalUrl) URL.revokeObjectURL(contentMediaLocalUrl);
    if (contentMediaServerUrl) URL.revokeObjectURL(contentMediaServerUrl);
    if (contentMediaAttachedCoverUrl) URL.revokeObjectURL(contentMediaAttachedCoverUrl);
    contentMediaLocalUrl = null;
    contentMediaServerUrl = null;
    contentMediaAttachedCoverUrl = null;
  };
  const resetContentMediaDraft = () => {
    if (contentMediaLocalUrl) URL.revokeObjectURL(contentMediaLocalUrl);
    if (contentMediaServerUrl) URL.revokeObjectURL(contentMediaServerUrl);
    contentMediaLocalUrl = null;
    contentMediaServerUrl = null;
    contentMediaUploadId = null;
    contentCoverFile.value = "";
    contentVideoFile.value = "";
    contentMediaConfirmation.hidden = true;
    contentMediaPreview.replaceChildren(text("span", "Предварительный просмотр", "schedule-image-loading"));
    contentMediaSummary.replaceChildren();
    contentMediaMessage.textContent = "";
  };
  const resetScheduleUpload = () => {
    clearScheduleUploadUrls();
    scheduleUploadId = null;
    scheduleUploadFile.value = "";
    scheduleUploadConfirm.hidden = true;
    scheduleUploadPreview.replaceChildren(text("span", "Выберите изображение", "schedule-image-loading"));
    scheduleUploadMessage.textContent = "Сначала проверьте локальное изображение, затем отправьте его на безопасную проверку.";
  };
  const showScreen = (name) => {
    if (name !== "schedule" && name !== "schedule-details" && scheduleImageUrls.size) {
      clearScheduleImages();
    }
    if (name !== "schedule-upload" && (scheduleUploadLocalUrl || scheduleUploadServerUrl)) {
      clearScheduleUploadUrls();
    }
    if (name !== "content-details") {
      clearContentMediaUrls();
      contentMediaUploadId = null;
    }
    document.querySelectorAll("[data-screen]").forEach((node) => { node.hidden = node.dataset.screen !== name; });
    document.querySelectorAll("[data-nav]").forEach((node) => { node.classList.toggle("active", node.dataset.nav === name); });
  };
  const postAdmin = (path, body) => fetch(path, {
    method: "POST", headers: {Authorization: `Bearer ${sessionToken}`}, body,
    cache: "no-store", credentials: "omit",
  }).then(async (response) => {
    if (response.status === 401) throw new Error("session_ended");
    if (response.status === 403) throw new Error("access_revoked");
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || "api_failed");
    return data;
  });
  const postAdminJson = (path, body) => fetch(path, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${sessionToken}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
    cache: "no-store",
    credentials: "omit",
  }).then(async (response) => {
    if (response.status === 401) throw new Error("session_ended");
    if (response.status === 403) throw new Error("access_revoked");
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || "api_failed");
    return data;
  });
  const writeAdminJson = (method, path, body) => fetch(path, {
    method,
    headers: {Authorization: `Bearer ${sessionToken}`, "Content-Type": "application/json"},
    body: JSON.stringify(body), cache: "no-store", credentials: "omit",
  }).then(async (response) => {
    if (response.status === 401) throw new Error("session_ended");
    if (response.status === 403) throw new Error("access_revoked");
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || "api_failed");
    return data;
  });
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
  const resetManualAccess = () => {
    manualAccessActionId = null;
    manualAccessControls.hidden = false;
    manualAccessConfirmation.hidden = true;
    manualAccessSummary.replaceChildren();
    manualAccessWarnings.replaceChildren();
    manualAccessMessage.textContent = "";
    manualAccessConfirm.disabled = false;
    manualAccessCancel.disabled = false;
  };
  const configureManualAccess = (user) => {
    manualAccessUserId = user.telegram_id;
    resetManualAccess();
    manualAccessCard.hidden = false;
  };
  const previewManualAccess = () => {
    if (!manualAccessUserId) return Promise.resolve();
    status.textContent = "Готовим изменение доступа…";
    return postAdminJson(
      `/api/admin/users/${encodeURIComponent(manualAccessUserId)}/access-preview`,
      {duration: manualAccessDuration.value}
    ).then((preview) => {
      manualAccessActionId = preview.action_id;
      const operation = preview.operation === "extend" ? "Продлить" : "Выдать";
      manualAccessSummary.replaceChildren(
        text("strong", preview.username ? `@${preview.username}` : `ID ${preview.telegram_id}`),
        text("span", `Telegram ID: ${preview.telegram_id}`),
        text("span", `Текущий доступ: ${formatDate(preview.current_expiry)}`),
        text("span", `После изменения: ${formatDate(preview.proposed_expiry)}`),
        text("span", `Будет выполнено: ${operation} доступ (${preview.duration})`)
      );
      manualAccessWarnings.replaceChildren();
      preview.warnings.forEach((warning) => manualAccessWarnings.append(text("p", `⚠ ${warning}`, "hint")));
      manualAccessControls.hidden = true;
      manualAccessConfirmation.hidden = false;
      manualAccessMessage.textContent = "Подтвердите ручное изменение локального доступа.";
      status.textContent = "Подтвердите изменение доступа";
    }).catch(showApiError);
  };
  const confirmManualAccess = () => {
    if (!manualAccessUserId || !manualAccessActionId) return Promise.resolve();
    manualAccessConfirm.disabled = true;
    return postAdminJson(
      `/api/admin/users/${encodeURIComponent(manualAccessUserId)}/access-confirm`,
      {action_id: manualAccessActionId}
    ).then(() => {
      status.textContent = "Доступ обновлён.";
      return loadUserDetails(manualAccessUserId);
    }).catch((error) => {
      manualAccessConfirm.disabled = false;
      manualAccessMessage.textContent = error.message === "manual_access_preview_expired"
        ? "Предварительный расчёт устарел. Обновите данные и повторите."
        : error.message === "user_access_state_changed"
          ? "Состояние пользователя изменилось. Обновите данные и повторите."
          : "Не удалось изменить доступ.";
    });
  };
  const cancelManualAccess = () => {
    if (!manualAccessUserId || !manualAccessActionId) {
      resetManualAccess(); return Promise.resolve();
    }
    manualAccessCancel.disabled = true;
    return postAdminJson(
      `/api/admin/users/${encodeURIComponent(manualAccessUserId)}/access-cancel`,
      {action_id: manualAccessActionId}
    ).then(() => {
      resetManualAccess(); status.textContent = "Изменение доступа отменено";
    }).catch(() => {
      manualAccessCancel.disabled = false;
      status.textContent = "Не удалось отменить запрос. Обновите данные.";
    });
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
      configureManualAccess(user);
      showScreen("user-details");
      status.textContent = "Профиль пользователя";
    }).catch(showApiError);
  }

  const subscriptionStateLabels = {
    active_grace: "Активный grace", failed_payment: "Ошибка оплаты",
    active_renewing: "Активна", active_non_renewing: "Не продлевается",
    expired_grace: "Grace истёк", expired: "Истекла", inactive: "Неактивна",
    unknown: "Статус неизвестен",
  };
  const formatDate = (value) => value ? new Date(value).toLocaleString("ru-RU") : "—";
  const subscriptionCard = (subscription) => {
    const article = document.createElement("article");
    article.className = `card user-card${subscription.needs_attention ? " attention" : ""}`;
    const button = document.createElement("button");
    button.type = "button";
    button.append(text("h2", subscription.username ? `@${subscription.username}` : (subscription.first_name || "Без username")));
    button.append(text("p", `Telegram ID: ${subscription.telegram_id}`));
    button.append(text("p", `Доступ до: ${formatDate(subscription.expiry_date)}`));
    const badges = document.createElement("div");
    badges.className = "badges";
    badges.append(text("span", subscriptionStateLabels[subscription.subscription_state] || subscription.subscription_state, "badge"));
    if (subscription.auto_renew) badges.append(text("span", "Автопродление", "badge"));
    if (subscription.grace_period_end) badges.append(text("span", `Grace до ${formatDate(subscription.grace_period_end)}`, "badge"));
    if (subscription.needs_attention) badges.append(text("span", "⚠️ Требует внимания", "badge attention-label"));
    button.append(badges);
    button.addEventListener("click", () => loadSubscriptionDetails(subscription.telegram_id));
    article.append(button);
    return article;
  };
  const loadSubscriptions = (append = false) => {
    status.textContent = "Загружаем подписки…";
    const params = new URLSearchParams({limit: "25", state: subscriptionsState.value});
    if (subscriptionsSearch.value.trim()) params.set("q", subscriptionsSearch.value.trim());
    if (append && subscriptionsCursor) params.set("cursor", subscriptionsCursor);
    return api(`/api/admin/subscriptions?${params.toString()}`).then((data) => {
      if (!append) subscriptionsList.replaceChildren();
      data.items.forEach((subscription) => subscriptionsList.append(subscriptionCard(subscription)));
      subscriptionsCursor = data.next_cursor;
      subscriptionsMore.hidden = !data.has_more;
      subscriptionMetricNodes.forEach((node) => {
        node.textContent = String(data.summary[node.dataset.subscriptionMetric] ?? "—");
      });
      showScreen("subscriptions");
      status.textContent = `Подписок показано: ${subscriptionsList.children.length}`;
    });
  };
  function loadSubscriptionDetails(userId) {
    status.textContent = "Загружаем подписку…";
    return api(`/api/admin/subscriptions/${encodeURIComponent(userId)}`).then((subscription) => {
      subscriptionDetailsContent.replaceChildren();
      subscriptionDetailsContent.append(
        detailCard("Пользователь", [["Telegram ID", subscription.telegram_id], ["Username", subscription.username ? `@${subscription.username}` : "—"], ["Имя", [subscription.first_name, subscription.last_name].filter(Boolean).join(" ") || "—"]]),
        detailCard("Доступ", [["Статус", statusLabels[subscription.access_status] || subscription.access_status], ["Тип", typeLabels[subscription.access_type] || subscription.access_type], ["До", formatDate(subscription.expiry_date)], ["Состояние подписки", subscriptionStateLabels[subscription.subscription_state] || subscription.subscription_state]]),
        detailCard("Оплата", [["Paid", subscription.paid ? "Да" : "Нет"], ["Автопродление", subscription.auto_renew ? "Да" : "Нет"], ["Ошибка оплаты", subscription.payment_failed ? "Да" : "Нет"], ["Ошибка с", formatDate(subscription.payment_failed_at)], ["Grace до", formatDate(subscription.grace_period_end)], ["Первый платёж", subscription.first_payment_done ? "Да" : "Нет"], ["Trial использован", subscription.trial_used ? "Да" : "Нет"]]),
        detailCard("Stripe linkage", [["Customer", subscription.stripe.customer_id], ["Subscription", subscription.stripe.subscription_id]]),
        detailCard("Удаление", subscription.removal ? [["Статус", subscription.removal.status], ["Причина", subscription.removal.reason], ["Access expiry", formatDate(subscription.removal.access_expiry)], ["Stripe отменена", formatDate(subscription.removal.stripe_canceled_at)], ["Telegram ban", formatDate(subscription.removal.telegram_banned_at)], ["Обновлено", formatDate(subscription.removal.updated_at)]] : [["Статус", "Нет операции"]]),
        detailCard("История доступа", subscription.access_history.length ? subscription.access_history.map((event) => [event.event_type, `${event.source}: ${formatDate(event.old_expiry)} → ${formatDate(event.new_expiry)}`]) : [["События", "Нет"]]),
        detailCard("История оплаты", subscription.payment_history.length ? subscription.payment_history.map((event) => [event.event_type, `${event.payment_status} · ${event.payment_kind} · ${event.tariff_code}`]) : [["События", "Нет"]])
      );
      showScreen("subscription-details");
      status.textContent = "Подписка пользователя";
    }).catch(showApiError);
  }

  const replaceDefinitionList = (container, pairs) => {
    container.replaceChildren();
    pairs.forEach(([label, value]) => {
      const row = document.createElement("div");
      row.append(text("dt", label), text("dd", value));
      container.append(row);
    });
  };
  const deliveryCard = (delivery) => {
    const article = document.createElement("article");
    article.className = `card user-card${delivery.requires_attention ? " attention" : ""}`;
    const button = document.createElement("button");
    button.type = "button";
    button.append(text("h2", delivery.delivery_label));
    button.append(text("p", `Telegram ID: ${delivery.telegram_id}`));
    button.append(text("p", `Reference: ${delivery.delivery_reference}`));
    const badges = document.createElement("div");
    badges.className = "badges";
    badges.append(text("span", delivery.status, "badge"));
    badges.append(text("span", `Попыток: ${delivery.attempt_count}`, "badge"));
    if (delivery.requires_attention) badges.append(text("span", "⚠️ Требует внимания", "badge attention-label"));
    button.append(badges);
    if (delivery.explanation) button.append(text("p", delivery.explanation));
    button.addEventListener("click", () => loadDeliveryDetails(delivery.delivery_id));
    article.append(button);
    return article;
  };
  const loadDeliveries = (append = false) => {
    const params = new URLSearchParams({limit: "25", status: deliveriesStatus.value});
    if (append && deliveriesCursor) params.set("cursor", deliveriesCursor);
    return api(`/api/admin/deliveries?${params.toString()}`).then((data) => {
      if (!append) deliveriesList.replaceChildren();
      data.items.forEach((delivery) => deliveriesList.append(deliveryCard(delivery)));
      deliveriesCursor = data.next_cursor;
      deliveriesMore.hidden = !data.has_more;
    });
  };
  const loadSystem = () => {
    status.textContent = "Загружаем состояние системы…";
    return Promise.all([api("/api/admin/system"), loadDeliveries(false)]).then(([data]) => {
      systemMetricNodes.forEach((node) => {
        if (node.dataset.systemMetric === "database") {
          node.textContent = data.database.connection_errors ? "Проблема" : "OK";
        } else {
          node.textContent = String(valueAtPath(data, node.dataset.systemMetric) ?? "—");
        }
      });
      replaceDefinitionList(systemAttention, [
        ["Permanently failed deliveries", data.deliveries.permanently_failed],
        ["Failed scheduler jobs 24h", data.scheduler.failed_last_24h],
        ["Retryable removals", data.removals.retryable],
      ]);
      replaceDefinitionList(systemDeliveryMetrics, [
        ["Pending", data.deliveries.pending], ["Processing", data.deliveries.processing],
        ["Failed", data.deliveries.failed], ["Permanently failed", data.deliveries.permanently_failed],
        ["Sent 24h", data.deliveries.sent_last_24h],
      ]);
      replaceDefinitionList(systemMigrations, [
        ["Количество", data.migrations.count], ["Последняя", data.migrations.latest],
        ["Применена", formatDate(data.migrations.latest_applied_at)],
      ]);
      schedulerRuns.replaceChildren();
      data.scheduler.recent_runs.forEach((run) => {
        const item = document.createElement("div");
        item.className = "compact-item";
        item.append(text("strong", run.job_name), text("span", run.status));
        if (run.stale) item.append(text("span", "⚠️ Просроченный lease", "attention-label"));
        if (run.error.category) item.append(text("span", `${run.error.category} · ${run.error.reference}`));
        schedulerRuns.append(item);
      });
      showScreen("system");
      status.textContent = "Состояние системы";
    });
  };
  function loadDeliveryDetails(deliveryId) {
    status.textContent = "Загружаем доставку…";
    return api(`/api/admin/deliveries/${encodeURIComponent(deliveryId)}`).then((delivery) => {
      deliveryDetailsContent.replaceChildren();
      deliveryDetailsContent.append(
        detailCard("Доставка", [["Reference", delivery.delivery_reference], ["Тип", delivery.delivery_label], ["Статус", delivery.status], ["Telegram ID", delivery.telegram_id], ["Попытки", delivery.attempt_count]]),
        detailCard("Время", [["Claimed", formatDate(delivery.claimed_at)], ["Lease до", formatDate(delivery.lease_until)], ["Следующая попытка", formatDate(delivery.next_attempt_at)], ["Отправлено", formatDate(delivery.sent_at)]]),
        detailCard("Ошибка", [["Категория", delivery.last_error.category], ["Safe reference", delivery.last_error.reference], ["Пояснение", delivery.explanation]])
      );
      showScreen("delivery-details");
      status.textContent = "Событие доставки";
    }).catch(showApiError);
  }

  const moscowDate = () => {
    const parts = new Intl.DateTimeFormat("en-CA", {
      timeZone: "Europe/Moscow", year: "numeric", month: "2-digit", day: "2-digit",
    }).formatToParts(new Date()).reduce((result, part) => {
      result[part.type] = part.value;
      return result;
    }, {});
    return `${parts.year}-${parts.month}-${parts.day}`;
  };
  const addDays = (isoDate, days) => {
    const date = new Date(`${isoDate}T00:00:00Z`);
    date.setUTCDate(date.getUTCDate() + days);
    return date.toISOString().slice(0, 10);
  };
  const scheduleParams = (append) => {
    const today = moscowDate();
    const params = new URLSearchParams({limit: "25", status: "all"});
    if (scheduleRange === "archive") {
      params.set("period", "archive");
    } else if (scheduleRange === "today") {
      params.set("from", today); params.set("to", today);
    } else if (scheduleRange === "7" || scheduleRange === "30") {
      params.set("from", today); params.set("to", addDays(today, Number(scheduleRange)));
    } else {
      params.set("from", today); params.set("to", addDays(today, 730));
      params.set("status", "upcoming");
    }
    if (append && scheduleCursor) params.set("cursor", scheduleCursor);
    return params;
  };
  const fetchScheduleImage = (schedule, container, generation) => {
    const existing = scheduleImageUrls.get(schedule.schedule_id);
    if (existing) {
      const image = document.createElement("img");
      image.className = "schedule-image";
      image.alt = schedule.title;
      image.src = existing;
      container.replaceChildren(image);
      return Promise.resolve();
    }
    return fetch(`/api/admin/schedule/${encodeURIComponent(schedule.schedule_id)}/image`, {
      method: "GET", headers: {Authorization: `Bearer ${sessionToken}`},
      cache: "no-store", credentials: "omit",
    }).then((response) => {
      if (response.status === 401) throw new Error("session_ended");
      if (response.status === 403) throw new Error("access_revoked");
      if (!response.ok) throw new Error("schedule_image_failed");
      return response.blob();
    }).then((blob) => {
      if (generation !== scheduleImageGeneration) return;
      const objectUrl = URL.createObjectURL(blob);
      const previous = scheduleImageUrls.get(schedule.schedule_id);
      if (previous) URL.revokeObjectURL(previous);
      scheduleImageUrls.set(schedule.schedule_id, objectUrl);
      const image = document.createElement("img");
      image.className = "schedule-image";
      image.alt = schedule.title;
      image.src = objectUrl;
      container.replaceChildren(image);
    }).catch((error) => {
      if (generation !== scheduleImageGeneration) return;
      container.replaceChildren(text("span", "Не удалось загрузить изображение", "schedule-image-error"));
      if (error.message === "session_ended" || error.message === "access_revoked") {
        showApiError(error);
      }
    });
  };
  const scheduleImageContainer = (schedule, generation, large = false) => {
    const container = document.createElement("div");
    container.className = large ? "schedule-preview schedule-preview-large" : "schedule-preview";
    container.append(text("span", "Загружаем расписание…", "schedule-image-loading"));
    fetchScheduleImage(schedule, container, generation);
    return container;
  };
  const scheduleCard = (schedule, generation) => {
    const article = document.createElement("article");
    article.className = "card user-card";
    const button = document.createElement("button");
    button.type = "button";
    button.append(text("p", schedule.period_label, "eyebrow"));
    button.append(text("h2", schedule.title));
    button.append(scheduleImageContainer(schedule, generation));
    const badges = document.createElement("div");
    badges.className = "badges";
    badges.append(text("span", schedule.status === "upcoming" ? "Опубликовано" : "Прошедшее", "badge"));
    badges.append(text("span", "Изображение загружено", "badge"));
    button.append(badges);
    button.append(text("p", `Обновлено: ${formatDate(schedule.updated_at)}`, "hint"));
    button.addEventListener("click", () => loadScheduleDetails(schedule.schedule_id));
    article.append(button);
    return article;
  };
  const loadSchedule = (append = false) => {
    status.textContent = "Загружаем расписание…";
    return api(`/api/admin/schedule?${scheduleParams(append).toString()}`).then((data) => {
      if (!append) {
        clearScheduleImages();
        scheduleList.replaceChildren();
      }
      const generation = scheduleImageGeneration;
      data.items.forEach((schedule) => scheduleList.append(scheduleCard(schedule, generation)));
      scheduleCursor = data.next_cursor;
      scheduleMore.hidden = !data.has_more;
      scheduleEmpty.hidden = scheduleList.children.length !== 0;
      const archive = scheduleRange === "archive";
      scheduleHeading.textContent = archive ? "Архив расписаний" : "Расписание";
      scheduleEmpty.textContent = archive
        ? "Архивных расписаний пока нет."
        : "На выбранный период расписаний нет.";
      scheduleMetricNodes.forEach((node) => {
        node.textContent = String(data.summary[node.dataset.scheduleMetric] ?? "—");
      });
      showScreen("schedule");
      status.textContent = data.items.length
        ? (archive ? "Архив расписаний" : "Расписание клуба")
        : scheduleEmpty.textContent;
    });
  };
  function loadScheduleDetails(scheduleId) {
    status.textContent = "Загружаем расписание…";
    return api(`/api/admin/schedule/${encodeURIComponent(scheduleId)}`).then((schedule) => {
      scheduleDetailsContent.replaceChildren();
      scheduleDetailsContent.append(
        scheduleImageContainer(schedule, scheduleImageGeneration, true),
        detailCard("Основное", [["Название", schedule.title], ["Период", schedule.period_label], ["Статус", schedule.published ? "Опубликовано" : "Не опубликовано"]]),
        detailCard("Тип", [["Источник", "Telegram-изображение"], ["Изображение", schedule.has_image ? "Настроено" : "Нет"], ["Join link", schedule.has_join_link ? "Настроена" : "Нет"]]),
        detailCard("Описание", [["Описание", schedule.description || "Отдельное описание не хранится"]]),
        detailCard("Техническая информация", [["Timezone", schedule.timezone], ["Создано", formatDate(schedule.created_at)], ["Обновлено", formatDate(schedule.updated_at)], ["Состояние", schedule.technical_information]])
      );
      showScreen("schedule-details");
      status.textContent = schedule.period_label;
    }).catch(showApiError);
  }

  const showScheduleUploadImage = (url, alt) => {
    const image = document.createElement("img");
    image.className = "schedule-image";
    image.alt = alt;
    image.src = url;
    scheduleUploadPreview.replaceChildren(image);
  };
  const openScheduleUpload = () => {
    resetScheduleUpload();
    scheduleUploadMonth.value = new Date().toISOString().slice(0, 7);
    showScreen("schedule-upload");
    status.textContent = "Выберите месяц и изображение";
  };
  const validateScheduleUpload = () => {
    const file = scheduleUploadFile.files[0];
    if (!scheduleUploadMonth.value || !file) {
      scheduleUploadMessage.textContent = "Выберите месяц и изображение.";
      return Promise.resolve();
    }
    const form = new FormData();
    form.append("month", scheduleUploadMonth.value);
    form.append("image", file);
    scheduleUploadMessage.textContent = "Проверяем изображение…";
    scheduleUploadConfirm.hidden = true;
    return postAdmin("/api/admin/schedule/upload-preview", form).then((draft) => {
      scheduleUploadId = draft.upload_id;
      return fetch(`/api/admin/schedule/uploads/${encodeURIComponent(draft.upload_id)}/image`, {
        headers: {Authorization: `Bearer ${sessionToken}`}, cache: "no-store", credentials: "omit",
      }).then((response) => {
        if (!response.ok) throw new Error("preview_failed");
        return response.blob();
      }).then((blob) => {
        if (scheduleUploadServerUrl) URL.revokeObjectURL(scheduleUploadServerUrl);
        scheduleUploadServerUrl = URL.createObjectURL(blob);
        showScheduleUploadImage(scheduleUploadServerUrl, `Расписание ${draft.schedule_month}`);
        scheduleUploadMessage.textContent = draft.existing_schedule
          ? "Расписание за этот месяц уже существует. После подтверждения текущая картинка будет заменена."
          : "Будет создано новое расписание.";
        scheduleUploadConfirm.hidden = false;
      });
    }).catch((error) => {
      scheduleUploadMessage.textContent = error.message === "schedule_image_too_large"
        ? "Изображение превышает 10 МиБ."
        : "Изображение не прошло безопасную проверку.";
    });
  };
  const confirmScheduleUpload = () => {
    if (!scheduleUploadId) return Promise.resolve();
    scheduleUploadConfirm.disabled = true;
    scheduleUploadMessage.textContent = "Загружаем расписание…";
    return postAdmin(`/api/admin/schedule/uploads/${encodeURIComponent(scheduleUploadId)}/confirm`).then((result) => {
      if (result.status !== "completed") throw new Error(result.failure_category || result.status);
      const month = scheduleUploadMonth.value;
      resetScheduleUpload();
      status.textContent = `Расписание за ${month} обновлено.`;
      return loadSchedule(false);
    }).catch((error) => {
      scheduleUploadMessage.textContent = error.message === "stale_schedule_preview"
        ? "Расписание изменилось после предварительного просмотра. Обновите данные и повторите."
        : "Не удалось обновить расписание. Повторите безопасную проверку.";
    }).finally(() => { scheduleUploadConfirm.disabled = false; });
  };
  const cancelScheduleUploadDraft = () => {
    const request = scheduleUploadId
      ? postAdmin(`/api/admin/schedule/uploads/${encodeURIComponent(scheduleUploadId)}/cancel`).catch(() => null)
      : Promise.resolve();
    return request.then(() => { resetScheduleUpload(); return loadSchedule(false); });
  };

  const giftProfileLabel = (profile) => {
    if (!profile) return "Не указан";
    const name = [profile.first_name, profile.last_name].filter(Boolean).join(" ");
    const username = profile.username ? `@${profile.username}` : null;
    return [username, name, `ID ${profile.telegram_id}`].filter(Boolean).join(" · ");
  };
  const giftCard = (gift) => {
    const article = document.createElement("article");
    article.className = `card user-card${gift.requires_attention ? " attention" : ""}`;
    const button = document.createElement("button");
    button.type = "button";
    button.append(text("p", gift.public_reference, "eyebrow"));
    button.append(text("h2", `Подарок на ${gift.duration_label}`));
    const badges = document.createElement("div");
    badges.className = "badges";
    badges.append(text("span", gift.status_label, "badge"));
    if (gift.requires_attention) badges.append(text("span", "⚠️ Требует внимания", "badge attention-label"));
    button.append(badges);
    button.append(
      text("p", `Получатель: ${giftProfileLabel(gift.recipient)}`),
      text("p", `Покупатель: ${giftProfileLabel(gift.purchaser)}`),
      text("p", `Имя на сертификате: ${gift.certificate_name || "Без имени"}`),
      text("p", `Создан: ${formatDate(gift.created_at)}`)
    );
    if (gift.redeemed_at) button.append(text("p", `Активирован: ${formatDate(gift.redeemed_at)}`));
    button.addEventListener("click", () => loadGiftDetails(gift.gift_id));
    article.append(button);
    return article;
  };
  const loadGifts = (append = false) => {
    status.textContent = "Загружаем подарки…";
    const params = new URLSearchParams({
      limit: "25", status: giftsStatus.value, duration: giftsDuration.value,
    });
    if (giftsSearch.value.trim()) params.set("q", giftsSearch.value.trim());
    if (append && giftsCursor) params.set("cursor", giftsCursor);
    return api(`/api/admin/gifts?${params.toString()}`).then((data) => {
      if (!append) giftsList.replaceChildren();
      data.items.forEach((gift) => giftsList.append(giftCard(gift)));
      giftsCursor = data.next_cursor;
      giftsMore.hidden = !data.has_more;
      giftsEmpty.hidden = giftsList.children.length !== 0;
      giftMetricNodes.forEach((node) => {
        node.textContent = String(data.summary[node.dataset.giftMetric] ?? "—");
      });
      showScreen("gifts");
      status.textContent = data.items.length ? `Подарков показано: ${giftsList.children.length}` : "Подарков пока нет.";
    });
  };
  const resetGiftResend = () => {
    giftResendActionId = null;
    giftResendConfirmation.hidden = true;
    giftResendControls.hidden = false;
    giftResendSummary.replaceChildren();
    giftResendMessage.textContent = "";
    giftResendConfirm.disabled = false;
    giftResendConfirm.hidden = false;
  };
  const configureGiftResend = (gift) => {
    giftResendGiftId = gift.gift_id;
    resetGiftResend();
    giftResendCard.hidden = false;
    giftResendTarget.replaceChildren();
    const resend = gift.resend || {eligible: false, targets: []};
    giftResendUnavailable.hidden = resend.eligible;
    giftResendControls.hidden = !resend.eligible;
    if (!resend.eligible) return;
    resend.targets.forEach((target) => {
      const option = document.createElement("option");
      option.value = target;
      option.textContent = target === "purchaser" ? "Покупателю" : "Получателю";
      giftResendTarget.append(option);
    });
  };
  const previewGiftResend = () => {
    if (!giftResendGiftId || !giftResendTarget.value) return Promise.resolve();
    status.textContent = "Готовим подтверждение…";
    return postAdminJson(
      `/api/admin/gifts/${encodeURIComponent(giftResendGiftId)}/resend-preview`,
      {target: giftResendTarget.value}
    ).then((preview) => {
      giftResendActionId = preview.action_id;
      giftResendSummary.replaceChildren(
        text("strong", `Подарок на ${preview.duration}`),
        text("span", `${preview.target_type}: ${preview.target_username ? `@${preview.target_username}` : `Telegram ID ${preview.target_telegram_id}`}`),
        text("span", `Имя на сертификате: ${preview.certificate_name || "Без имени"}`),
        text("span", `Будет отправлено: ${preview.delivery_kind}`)
      );
      giftResendMessage.textContent = preview.confirmation_text;
      giftResendControls.hidden = true;
      giftResendConfirmation.hidden = false;
      status.textContent = "Подтвердите повторную отправку";
    }).catch((error) => {
      status.textContent = error.message === "gift_state_not_resendable"
        ? "Повторная отправка недоступна для текущего состояния подарка."
        : "Не удалось подготовить повторную отправку.";
    });
  };
  const confirmGiftResend = () => {
    if (!giftResendGiftId || !giftResendActionId) return Promise.resolve();
    giftResendConfirm.disabled = true;
    status.textContent = "Ставим сообщение в очередь…";
    return postAdminJson(
      `/api/admin/gifts/${encodeURIComponent(giftResendGiftId)}/resend-confirm`,
      {action_id: giftResendActionId}
    ).then((result) => {
      giftResendMessage.textContent = result.delivery_status === "queued"
        ? "Сообщение поставлено в очередь на отправку."
        : "Действие уже обработано.";
      giftResendConfirm.hidden = true;
      status.textContent = "Повторная отправка поставлена в очередь";
    }).catch((error) => {
      giftResendConfirm.disabled = false;
      giftResendMessage.textContent = error.message === "gift_state_changed"
        ? "Состояние подарка изменилось. Обновите данные и повторите."
        : error.message === "gift_target_changed"
          ? "Получатель подарка изменился. Обновите данные и повторите."
          : "Не удалось поставить сообщение в очередь.";
      status.textContent = "Повторная отправка не выполнена";
    });
  };
  const cancelGiftResend = () => {
    if (!giftResendGiftId || !giftResendActionId) {
      resetGiftResend();
      return Promise.resolve();
    }
    giftResendCancel.disabled = true;
    status.textContent = "Отменяем запрос…";
    return postAdminJson(
      `/api/admin/gifts/${encodeURIComponent(giftResendGiftId)}/resend-cancel`,
      {action_id: giftResendActionId}
    ).then(() => {
      resetGiftResend();
      status.textContent = "Повторная отправка отменена";
    }).catch(() => {
      status.textContent = "Не удалось отменить запрос. Обновите данные.";
    }).finally(() => {
      giftResendCancel.disabled = false;
    });
  };
  function loadGiftDetails(giftId) {
    status.textContent = "Загружаем подарок…";
    return api(`/api/admin/gifts/${encodeURIComponent(giftId)}`).then((gift) => {
      giftDetailsContent.replaceChildren();
      const lifecycle = gift.lifecycle_events.length
        ? gift.lifecycle_events.map((event) => [event.event_type, `${event.source || "—"} · ${formatDate(event.created_at)}`])
        : [["События", "Нет"]];
      giftDetailsContent.append(
        detailCard("Подарок", [["Reference", gift.public_reference], ["Статус", gift.status_label], ["Тариф", gift.duration_label], ["Создан", formatDate(gift.created_at)], ["Оплачен", formatDate(gift.paid_at)]]),
        detailCard("Покупатель", [["Профиль", giftProfileLabel(gift.purchaser)]]),
        detailCard("Получатель", [["Профиль", giftProfileLabel(gift.recipient)], ["Указанное имя", gift.recipient_name], ["Имя на сертификате", gift.certificate_name || "Без имени"]]),
        detailCard("Активация", [["Reserved", formatDate(gift.reserved_at)], ["Активирован", formatDate(gift.redeemed_at)], ["Применён", formatDate(gift.applied_at)], ["Доступ до", formatDate(gift.applied_expiry)]]),
        detailCard("Завершение", [["Возвращён", formatDate(gift.refunded_at)], ["Отменён", formatDate(gift.cancelled_at)]]),
        detailCard("Lifecycle", lifecycle)
      );
      configureGiftResend(gift);
      showScreen("gift-details");
      status.textContent = gift.public_reference;
    }).catch(showApiError);
  }

  const contentCard = (item) => {
    const article = document.createElement("article");
    article.className = "card user-card";
    const button = document.createElement("button");
    button.type = "button";
    button.append(text("p", item.category_label, "eyebrow"));
    button.append(text("h2", item.title));
    const badges = document.createElement("div");
    badges.className = "badges";
    badges.append(text("span", item.media_type, "badge"));
    badges.append(text("span", item.availability === "configured" ? "Настроен" : "Нет медиа", "badge"));
    button.append(badges);
    button.append(text("p", item.short_description));
    if (item.duration_minutes) button.append(text("p", `Длительность: ${item.duration_minutes} мин.`));
    button.addEventListener("click", () => loadContentDetails(item.content_id));
    article.append(button);
    return article;
  };
  const loadContent = () => {
    status.textContent = "Загружаем контент…";
    const params = new URLSearchParams({category: contentCategory.value});
    if (contentSearch.value.trim()) params.set("q", contentSearch.value.trim());
    return Promise.all([
      api(`/api/admin/content?${params.toString()}`),
      api("/api/admin/content/cms?status=all&limit=50"),
    ]).then(([data, cms]) => {
      contentList.replaceChildren();
      data.items.forEach((item) => contentList.append(contentCard(item)));
      contentEmpty.hidden = data.items.length !== 0;
      cmsContentList.replaceChildren();
      cms.items.forEach((item) => cmsContentList.append(cmsContentCard(item)));
      cmsContentEmpty.hidden = cms.items.length !== 0;
      showScreen("content");
      status.textContent = `Legacy: ${data.items.length} · CMS: ${cms.items.length}`;
    });
  };
  function loadContentDetails(contentId) {
    currentCmsContent = null;
    contentEditCard.hidden = true;
    status.textContent = "Загружаем материал…";
    return api(`/api/admin/content/${encodeURIComponent(contentId)}`).then((item) => {
      contentDetailsContent.replaceChildren(detailCard("Материал", [
        ["Название", item.title],
        ["Тип", item.content_type],
        ["Категория", item.category_label],
        ["Описание", item.short_description],
        ["Длительность", item.duration_minutes ? `${item.duration_minutes} мин.` : null],
        ["Порядок", item.ordering],
        ["Медиа", item.media_type],
        ["Медиа настроено", item.has_media ? "Да" : "Нет"],
        ["Состояние", item.availability === "configured" ? "Настроен" : "Нет медиа"],
      ]));
      showScreen("content-details");
      status.textContent = item.title;
    }).catch(showApiError);
  }

  const cmsContentCard = (item) => {
    const article = document.createElement("article");
    article.className = "card user-card";
    const button = document.createElement("button");
    button.type = "button";
    button.append(text("p", "CMS", "eyebrow"), text("h2", item.title));
    const badges = document.createElement("div");
    badges.className = "badges";
    badges.append(text("span", item.status, "badge"), text("span", `v${item.version}`, "badge"));
    button.append(badges, text("p", item.category || "Без категории"));
    button.addEventListener("click", () => loadCmsContentDetails(item.content_id));
    article.append(button);
    return article;
  };
  const loadCmsContentDetails = (contentId) => {
    clearContentMediaUrls();
    resetContentMediaDraft();
    status.textContent = "Загружаем черновик…";
    return api(`/api/admin/content/cms/${encodeURIComponent(contentId)}`).then((item) => {
      currentCmsContent = item;
      contentDetailsContent.replaceChildren(detailCard("CMS material", [
        ["Название", item.title], ["Тип", item.content_type],
        ["Категория", item.category], ["Описание", item.description],
        ["Длительность", item.duration_seconds ? `${item.duration_seconds} сек.` : null],
        ["Статус", item.status], ["Версия", item.version],
      ]));
      contentEditCard.hidden = item.status !== "draft";
      contentMediaCard.hidden = item.status !== "draft";
      contentEditTitle.value = item.title;
      contentEditCategory.value = item.category || "";
      contentEditDescription.value = item.description || "";
      contentEditDuration.value = item.duration_seconds || "";
      contentEditOrder.value = item.sort_order;
      contentEditMessage.textContent = "Изменения сохраняются только по кнопке.";
      renderContentMedia(item);
      showScreen("content-details");
      status.textContent = item.title;
    }).catch(showApiError);
  };
  const mediaImage = (url, alt) => {
    const image = document.createElement("img");
    image.className = "schedule-image";
    image.alt = alt;
    image.src = url;
    return image;
  };
  const fetchContentCover = (item, media) => fetch(
    `/api/admin/content/cms/${encodeURIComponent(item.content_id)}/media/${encodeURIComponent(media.media_id)}`,
    {headers: {Authorization: `Bearer ${sessionToken}`}, cache: "no-store", credentials: "omit"}
  ).then((response) => {
    if (!response.ok) throw new Error("content_media_preview_failed");
    return response.blob();
  }).then((blob) => {
    if (contentMediaAttachedCoverUrl) URL.revokeObjectURL(contentMediaAttachedCoverUrl);
    contentMediaAttachedCoverUrl = URL.createObjectURL(blob);
    contentCoverCurrent.replaceChildren(mediaImage(contentMediaAttachedCoverUrl, `Обложка ${item.title}`));
  }).catch(() => {
    contentCoverCurrent.replaceChildren(text("span", "Обложка недоступна", "schedule-image-loading"));
  });
  function renderContentMedia(item) {
    const cover = (item.media || []).find((entry) => entry.media_type === "cover");
    const video = (item.media || []).find((entry) => entry.media_type === "video");
    contentCoverCurrent.replaceChildren(text("span", cover ? "Загружаем обложку…" : "Обложка не прикреплена", "schedule-image-loading"));
    contentVideoCurrent.textContent = video
      ? `MP4 · ${Math.ceil(video.size_bytes / 1024 / 1024)} МиБ · версия ${video.version}`
      : "Видео не прикреплено";
    if (cover) fetchContentCover(item, cover);
  }
  const showLocalContentMedia = (mediaType) => {
    const file = mediaType === "cover" ? contentCoverFile.files[0] : contentVideoFile.files[0];
    if (contentMediaLocalUrl) URL.revokeObjectURL(contentMediaLocalUrl);
    contentMediaLocalUrl = file ? URL.createObjectURL(file) : null;
    contentMediaPreview.replaceChildren();
    if (!file) contentMediaPreview.append(text("span", "Выберите файл", "schedule-image-loading"));
    else if (mediaType === "cover") contentMediaPreview.append(mediaImage(contentMediaLocalUrl, "Локальный просмотр обложки"));
    else contentMediaPreview.append(text("span", `Локально выбрано видео: ${file.name}`, "schedule-image-loading"));
  };
  const validateContentMedia = (mediaType) => {
    if (!currentCmsContent || currentCmsContent.status !== "draft") return Promise.resolve();
    const file = mediaType === "cover" ? contentCoverFile.files[0] : contentVideoFile.files[0];
    if (!file) {
      contentMediaMessage.textContent = "Выберите файл.";
      contentMediaConfirmation.hidden = false;
      return Promise.resolve();
    }
    const form = new FormData();
    form.append("media_type", mediaType);
    form.append("file", file);
    contentMediaMessage.textContent = "Проверяем файл…";
    contentMediaConfirmation.hidden = false;
    return postAdmin(`/api/admin/content/cms/${encodeURIComponent(currentCmsContent.content_id)}/media-preview`, form)
      .then((draft) => {
        contentMediaUploadId = draft.upload_id;
        contentMediaSummary.replaceChildren(
          text("strong", currentCmsContent.title),
          text("span", mediaType === "cover" ? "Обложка" : "Видео"),
          text("span", `${draft.mime_type} · ${Math.ceil(draft.size_bytes / 1024)} КиБ`)
        );
        contentMediaMessage.textContent = draft.existing_media
          ? "Текущий файл будет заменён после подтверждения."
          : "Файл будет прикреплён только после подтверждения.";
        if (mediaType !== "cover") return null;
        return fetch(`/api/admin/content/media/uploads/${encodeURIComponent(draft.upload_id)}/preview`, {
          headers: {Authorization: `Bearer ${sessionToken}`}, cache: "no-store", credentials: "omit",
        }).then((response) => {
          if (!response.ok) throw new Error("content_media_preview_failed");
          return response.blob();
        }).then((blob) => {
          if (contentMediaServerUrl) URL.revokeObjectURL(contentMediaServerUrl);
          contentMediaServerUrl = URL.createObjectURL(blob);
          contentMediaPreview.replaceChildren(mediaImage(contentMediaServerUrl, "Проверенная обложка"));
        });
      }).catch((error) => {
        contentMediaUploadId = null;
        contentMediaMessage.textContent = error.message === "content_media_too_large"
          ? "Файл превышает допустимый размер."
          : "Файл не прошёл безопасную проверку.";
      });
  };
  const confirmContentMedia = () => {
    if (!contentMediaUploadId || !currentCmsContent) return Promise.resolve();
    contentMediaConfirm.disabled = true;
    contentMediaMessage.textContent = "Загружаем и прикрепляем…";
    return postAdmin(`/api/admin/content/media/uploads/${encodeURIComponent(contentMediaUploadId)}/confirm`)
      .then((result) => {
        if (result.status !== "completed") throw new Error(result.failure_category || result.status);
        return loadCmsContentDetails(currentCmsContent.content_id);
      }).catch((error) => {
        contentMediaMessage.textContent = error.message === "content_version_changed"
          ? "Материал изменился после проверки. Обновите данные и повторите."
          : "Не удалось прикрепить файл. Повторите безопасную проверку.";
      }).finally(() => { contentMediaConfirm.disabled = false; });
  };
  const cancelContentMedia = () => {
    const request = contentMediaUploadId
      ? postAdmin(`/api/admin/content/media/uploads/${encodeURIComponent(contentMediaUploadId)}/cancel`).catch(() => null)
      : Promise.resolve();
    return request.then(() => { resetContentMediaDraft(); status.textContent = "Загрузка медиа отменена"; });
  };
  const optionalNumber = (input) => input.value === "" ? null : Number(input.value);
  const createCmsDraft = () => {
    contentCreateMessage.textContent = "Создаём черновик…";
    return writeAdminJson("POST", "/api/admin/content/drafts", {
      content_type: "lesson", title: contentCreateTitle.value,
      category: contentCreateCategory.value || null,
      description: contentCreateDescription.value || null,
      duration_seconds: optionalNumber(contentCreateDuration),
    }).then((item) => {
      contentCreateTitle.value = ""; contentCreateCategory.value = "";
      contentCreateDescription.value = ""; contentCreateDuration.value = "";
      return loadCmsContentDetails(item.content_id);
    }).catch((error) => {
      contentCreateMessage.textContent = `Не удалось создать черновик: ${error.message}`;
    });
  };
  const saveCmsDraft = () => {
    if (!currentCmsContent) return Promise.resolve();
    contentEditMessage.textContent = "Сохраняем…";
    return writeAdminJson(
      "PATCH", `/api/admin/content/cms/${encodeURIComponent(currentCmsContent.content_id)}`,
      {expected_version: currentCmsContent.version, title: contentEditTitle.value,
       category: contentEditCategory.value || null,
       description: contentEditDescription.value || null,
       duration_seconds: optionalNumber(contentEditDuration),
       sort_order: Number(contentEditOrder.value)}
    ).then((item) => loadCmsContentDetails(item.content_id)).catch((error) => {
      contentEditMessage.textContent = error.message === "content_version_changed"
        ? "Материал уже изменился. Обновите данные и повторите."
        : `Не удалось сохранить: ${error.message}`;
    });
  };

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
      else if (button.dataset.nav === "subscriptions") loadSubscriptions().catch(showApiError);
      else if (button.dataset.nav === "system") loadSystem().catch(showApiError);
      else if (button.dataset.nav === "schedule") loadSchedule().catch(showApiError);
      else showScreen(button.dataset.nav);
    });
  });
  refresh.addEventListener("click", () => loadDashboard().catch(showApiError));
  document.getElementById("open-gifts").addEventListener("click", () => loadGifts().catch(showApiError));
  document.getElementById("open-content").addEventListener("click", () => loadContent().catch(showApiError));
  document.getElementById("content-create-open").addEventListener("click", () => showScreen("content-create"));
  document.getElementById("content-create-back").addEventListener("click", () => loadContent().catch(showApiError));
  document.getElementById("content-create-submit").addEventListener("click", createCmsDraft);
  document.getElementById("content-edit-save").addEventListener("click", saveCmsDraft);
  document.getElementById("content-cover-validate").addEventListener("click", () => validateContentMedia("cover"));
  document.getElementById("content-video-validate").addEventListener("click", () => validateContentMedia("video"));
  contentCoverFile.addEventListener("change", () => showLocalContentMedia("cover"));
  contentVideoFile.addEventListener("change", () => showLocalContentMedia("video"));
  contentMediaConfirm.addEventListener("click", confirmContentMedia);
  contentMediaCancel.addEventListener("click", cancelContentMedia);
  usersMore.addEventListener("click", () => loadUsers(true).catch(showApiError));
  usersStatus.addEventListener("change", () => loadUsers().catch(showApiError));
  usersSearch.addEventListener("input", () => {
    window.clearTimeout(searchTimer);
    searchTimer = window.setTimeout(() => loadUsers().catch(showApiError), 300);
  });
  document.getElementById("users-back").addEventListener("click", () => showScreen("users"));
  subscriptionsMore.addEventListener("click", () => loadSubscriptions(true).catch(showApiError));
  subscriptionsState.addEventListener("change", () => loadSubscriptions().catch(showApiError));
  subscriptionsSearch.addEventListener("input", () => {
    window.clearTimeout(subscriptionsSearchTimer);
    subscriptionsSearchTimer = window.setTimeout(() => loadSubscriptions().catch(showApiError), 300);
  });
  document.getElementById("subscriptions-back").addEventListener("click", () => showScreen("subscriptions"));
  deliveriesMore.addEventListener("click", () => loadDeliveries(true).catch(showApiError));
  deliveriesStatus.addEventListener("change", () => loadDeliveries(false).catch(showApiError));
  document.getElementById("deliveries-back").addEventListener("click", () => showScreen("system"));
  scheduleMore.addEventListener("click", () => loadSchedule(true).catch(showApiError));
  document.querySelectorAll("[data-schedule-range]").forEach((button) => {
    button.addEventListener("click", () => {
      scheduleRange = button.dataset.scheduleRange;
      loadSchedule(false).catch(showApiError);
    });
  });
  document.getElementById("schedule-back").addEventListener("click", () => showScreen("schedule"));
  document.getElementById("schedule-upload-open").addEventListener("click", openScheduleUpload);
  document.getElementById("schedule-upload-back").addEventListener("click", cancelScheduleUploadDraft);
  document.getElementById("schedule-upload-cancel").addEventListener("click", cancelScheduleUploadDraft);
  document.getElementById("schedule-upload-validate").addEventListener("click", validateScheduleUpload);
  scheduleUploadConfirm.addEventListener("click", confirmScheduleUpload);
  scheduleUploadFile.addEventListener("change", () => {
    clearScheduleUploadUrls();
    scheduleUploadId = null;
    scheduleUploadConfirm.hidden = true;
    const file = scheduleUploadFile.files[0];
    if (file) {
      scheduleUploadLocalUrl = URL.createObjectURL(file);
      showScheduleUploadImage(scheduleUploadLocalUrl, "Локальное предварительное изображение расписания");
      scheduleUploadMessage.textContent = "Локальный просмотр готов. Нажмите «Проверить».";
    }
  });
  giftsMore.addEventListener("click", () => loadGifts(true).catch(showApiError));
  giftsStatus.addEventListener("change", () => loadGifts(false).catch(showApiError));
  giftsDuration.addEventListener("change", () => loadGifts(false).catch(showApiError));
  giftsSearch.addEventListener("input", () => {
    window.clearTimeout(giftsSearchTimer);
    giftsSearchTimer = window.setTimeout(() => loadGifts(false).catch(showApiError), 300);
  });
  document.getElementById("gift-resend-preview").addEventListener("click", previewGiftResend);
  giftResendConfirm.addEventListener("click", confirmGiftResend);
  giftResendCancel.addEventListener("click", cancelGiftResend);
  document.getElementById("manual-access-preview").addEventListener("click", previewManualAccess);
  manualAccessConfirm.addEventListener("click", confirmManualAccess);
  manualAccessCancel.addEventListener("click", cancelManualAccess);
  document.getElementById("gifts-back").addEventListener("click", () => showScreen("gifts"));
  document.getElementById("gifts-dashboard-back").addEventListener("click", () => loadDashboard().catch(showApiError));
  contentCategory.addEventListener("change", () => loadContent().catch(showApiError));
  contentSearch.addEventListener("input", () => {
    window.clearTimeout(contentSearchTimer);
    contentSearchTimer = window.setTimeout(() => loadContent().catch(showApiError), 300);
  });
  document.getElementById("content-back").addEventListener("click", () => showScreen("content"));
  document.getElementById("content-dashboard-back").addEventListener("click", () => loadDashboard().catch(showApiError));
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
