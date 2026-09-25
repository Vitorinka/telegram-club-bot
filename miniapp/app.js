(() => {
  "use strict";
  const contentStudioMediaPreflightError = (mediaType, file) => {
    if (!file) return null;
    const megabytes = (file.size / 1024 / 1024).toFixed(1);
    if (mediaType === "cover") {
      const extensionOk = /\.(jpe?g|png|webp)$/i.test(file.name);
      const mimeOk = ["image/jpeg", "image/png", "image/webp"].includes(file.type);
      if (!mimeOk && !extensionOk) return "Обложка должна быть JPEG, PNG или WebP.";
      if (file.size > 10 * 1024 * 1024) return `Обложка слишком большая: ${megabytes} МБ. Текущий максимум — 10 МБ.`;
    }
    if (mediaType === "video") {
      if (file.type !== "video/mp4" && !file.name.toLowerCase().endsWith(".mp4")) return "Этот формат пока не поддерживается. Загрузите MP4.";
      if (file.size > 20 * 1024 * 1024) return `Видео слишком большое: ${megabytes} МБ. Текущий максимум — 20 МБ.`;
    }
    if (mediaType === "audio") {
      if (file.type !== "audio/mpeg" && !file.name.toLowerCase().endsWith(".mp3")) return "Этот формат пока не поддерживается. Загрузите MP3.";
      if (file.size > 20 * 1024 * 1024) return `Аудио слишком большое: ${megabytes} МБ. Текущий максимум — 20 МБ.`;
    }
    return null;
  };
  const contentStudioCanStartMedia = (dirty) => !dirty;
  const contentStudioEffectiveCategory = (items, selected) => {
    const available = new Set(items.flatMap((item) => (item.categories || []).map((entry) => entry.id)));
    return selected !== "all" && available.has(selected) ? selected : "all";
  };
  const contentStudioCoverUrl = ({localUrl, localMediaType, attachedUrl}) =>
    localMediaType === "cover" && localUrl ? localUrl : attachedUrl;
  const contentStudioMove = (items, index, offset, onChange = () => {}) => {
    const target = index + offset;
    if (target < 0 || target >= items.length) return false;
    [items[index], items[target]] = [items[target], items[index]];
    onChange();
    return true;
  };
  const contentStudioSaveRecipe = ({saveMetadata, saveRecipe, reload}) =>
    saveMetadata().then(saveRecipe).then(reload);
  const adminScreenIsVisible = (activeScreen, candidateScreen) => activeScreen === candidateScreen;
  const adminSearchMatches = (values, query) => {
    const normalized=String(query || "").trim().toLocaleLowerCase("ru");
    return normalized.length >= 2 && values.some((value)=>String(value || "").toLocaleLowerCase("ru").includes(normalized));
  };
  const adminNotificationUnreadCount = (items, readKeys) => items.filter((item)=>!readKeys.has(item.key)).length;
  const adminSystemIncidentKey = (category, marker) => marker ? `system:${category}:${marker}` : null;
  const MAX_NOTIFICATION_PAGES = 100;
  const MAX_NOTIFICATION_PANEL_ITEMS = 25;
  const collectAdminNotificationKeys = async ({firstPage, fetchNext, keyFor, aggregate, maxPages=MAX_NOTIFICATION_PAGES}) => {
    const keys=new Set(); const items=new Map(); const cursors=new Set(); let page=firstPage; let pages=0;
    while (page) {
      pages += 1;
      (page.items || []).forEach((item)=>{ const key=keyFor(item); if (key) { keys.add(key); if (!items.has(key)) items.set(key,item); } });
      if (!page.has_more) return {keys,items:[...items.values()],complete:true,aggregate:Number(aggregate || 0),pages};
      const cursor=page.next_cursor;
      if (!cursor || cursors.has(cursor) || pages >= maxPages) return {keys,items:[...items.values()],complete:false,aggregate:Number(aggregate || 0),pages};
      cursors.add(cursor);
      try { page=await fetchNext(cursor); }
      catch (_error) { return {keys,items:[...items.values()],complete:false,aggregate:Number(aggregate || 0),pages}; }
    }
    return {keys,items:[...items.values()],complete:false,aggregate:Number(aggregate || 0),pages};
  };
  const adminNotificationUnknownUnread = (result) => result.complete ? 0 : Math.max(0,Number(result.aggregate || 0)-result.keys.size);
  const adminNotificationOverflowCount = (currentCount, panelItems) => Math.max(0,Number(currentCount || 0)-new Set(panelItems.map((item)=>item.key).filter(Boolean)).size);
  const orderAdminNotificationItems = (items) => {
    const unique=new Map();
    items.forEach((item)=>{ if (item && item.key && !unique.has(item.key)) unique.set(item.key,item); });
    return [...unique.values()].sort((left,right)=>{
      const leftTime=Date.parse(left.timestamp || ""); const rightTime=Date.parse(right.timestamp || "");
      const safeLeft=Number.isFinite(leftTime) ? leftTime : 0; const safeRight=Number.isFinite(rightTime) ? rightTime : 0;
      return safeRight-safeLeft || String(left.key).localeCompare(String(right.key));
    });
  };
  const adminNotificationPanelPage = (items, limit) => ({
    visible:items.slice(0,Math.max(0,limit)), remaining:Math.max(0,items.length-Math.max(0,limit)),
  });
  const createBoundedTaskQueue = (limit = 4) => {
    const waiting=[]; let active=0; let peak=0;
    const drain=()=>{
      while(active < limit && waiting.length){
        const task=waiting.shift(); active+=1; peak=Math.max(peak,active);
        Promise.resolve().then(task.run).then(task.resolve,task.reject).finally(()=>{ active-=1; drain(); });
      }
    };
    return {
      add:(run)=>new Promise((resolve,reject)=>{ waiting.push({run,resolve,reject}); drain(); }),
      clear:()=>{ waiting.splice(0).forEach((task)=>task.reject(new DOMException("Cancelled","AbortError"))); },
      stats:()=>({active,queued:waiting.length,peak}),
    };
  };
  const getOrCreateCachedResource = ({cache,pending,key,load,store}) => {
    if(cache.has(key)) return Promise.resolve(cache.get(key));
    if(pending.has(key)) return pending.get(key);
    let task;
    task=Promise.resolve().then(load).then((value)=>{ store(key,value); return value; }).finally(()=>{
      if(pending.get(key) === task) pending.delete(key);
    });
    pending.set(key,task); return task;
  };
  const hydrateAdminNotificationReadState = (readState, keys) => {
    readState.clear();
    (keys || []).forEach((key)=>readState.add(key));
    return readState;
  };
  const persistAdminNotificationRead = ({key,persist,readState}) => Promise.resolve(persist(key)).then(()=>{
    readState.add(key); return true;
  });
  const adminProfilePresentation = (user = {}) => {
    const displayName = [user.first_name,user.last_name].filter(Boolean).join(" ") || user.username || "Администратор";
    const initials = [user.first_name,user.last_name].filter(Boolean).map((part)=>String(part).slice(0,1)).join("").slice(0,2).toUpperCase() || "A";
    let photoUrl = null;
    try {
      const parsed = new URL(String(user.photo_url || ""));
      if (parsed.protocol === "https:") photoUrl = parsed.href;
    } catch (_error) { photoUrl = null; }
    return {displayName,initials,photoUrl};
  };
  const contentStudioCreateDraft = ({files, createDraft, saveDomain, attachMedia, openDraft}) => {
    const preflightError = files.map(([mediaType, file]) => contentStudioMediaPreflightError(mediaType, file)).find(Boolean);
    if (preflightError) return Promise.resolve({status: "preflight_failed", error: preflightError, draft: null});
    let draft;
    let stage = "create";
    return createDraft().then((created) => {
      draft = created;
      stage = "domain";
      return saveDomain(created);
    }).then(() => {
      stage = "media";
      return files.reduce(
        (chain, [mediaType, file]) => chain.then(() => attachMedia(draft, mediaType, file)),
        Promise.resolve(),
      );
    }).then(() => ({status: "completed", draft})).catch((error) => {
      if (!draft) throw error;
      const status = stage === "domain" ? "domain_failed" : "media_failed";
      return openDraft(draft).then(() => ({status, error, draft}));
    });
  };
  if (typeof module !== "undefined" && module.exports && typeof document === "undefined") {
    module.exports = {adminScreenIsVisible, adminSearchMatches, adminNotificationUnreadCount, adminSystemIncidentKey, collectAdminNotificationKeys, adminNotificationUnknownUnread, adminNotificationOverflowCount, orderAdminNotificationItems, adminNotificationPanelPage, createBoundedTaskQueue, getOrCreateCachedResource, hydrateAdminNotificationReadState, persistAdminNotificationRead, adminProfilePresentation, contentStudioCanStartMedia, contentStudioEffectiveCategory, contentStudioCoverUrl, contentStudioMediaPreflightError, contentStudioMove, contentStudioSaveRecipe, contentStudioCreateDraft};
    return;
  }
  const webApp = window.Telegram && window.Telegram.WebApp;
  const status = document.getElementById("status");
  const identity = document.getElementById("identity");
  const telegramId = document.getElementById("telegram-id");
  const refresh = document.getElementById("refresh");
  const adminHero = document.getElementById("admin-hero");
  const bottomNav = document.getElementById("bottom-nav");
  const adminFullscreen = document.getElementById("admin-fullscreen");
  const fullscreenMessage = document.getElementById("fullscreen-message");
  const adminGlobalSearch = document.getElementById("admin-global-search");
  const adminSearchResults = document.getElementById("admin-search-results");
  const adminNotifications = document.getElementById("admin-notifications");
  const adminNotificationPanel = document.getElementById("admin-notification-panel");
  const adminProfileToggle = document.getElementById("admin-profile-toggle");
  const adminProfilePanel = document.getElementById("admin-profile-panel");
  const adminContentNav = document.getElementById("admin-content-nav");
  const adminContentSubnav = document.getElementById("admin-content-subnav");
  const memberBottomNav = document.getElementById("member-bottom-nav");
  const memberShellHeader = document.getElementById("member-shell-header");
  const usersSearch = document.getElementById("users-search");
  const usersStatus = document.getElementById("users-status");
  const usersList = document.getElementById("users-list");
  const usersMore = document.getElementById("users-more");
  const userProfileHeader = document.getElementById("user-profile-header");
  const dashboardContentList = document.getElementById("dashboard-content-list");
  const dashboardUsersList = document.getElementById("dashboard-users-list");
  const dashboardScheduleList = document.getElementById("dashboard-schedule-list");
  const dashboardGiftsList = document.getElementById("dashboard-gifts-list");
  const dashboardFailedList = document.getElementById("dashboard-failed-list");
  const attentionList = document.getElementById("attention-list");
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
  const failedSubscriptionsFilter = document.getElementById("failed-subscriptions-filter");
  const failedSubscriptionsList = document.getElementById("failed-subscriptions-list");
  const failedSubscriptionsEmpty = document.getElementById("failed-subscriptions-empty");
  const failedSubscriptionsMore = document.getElementById("failed-subscriptions-more");
  const failedSubscriptionDetailsContent = document.getElementById("failed-subscription-details-content");
  const failedSubscriptionMetricNodes = document.querySelectorAll("[data-failed-metric]");
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
  const classCalendarList = document.getElementById("class-calendar-list");
  const classCreateForm = document.getElementById("class-create-form");
  let editingClassId = null;
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
  const contentType = document.getElementById("content-type");
  const contentStatusFilters = document.getElementById("content-status-filters");
  const contentList = document.getElementById("content-list");
  const contentEmpty = document.getElementById("content-empty");
  const contentDetailsContent = document.getElementById("content-details-content");
  const cmsContentList = document.getElementById("cms-content-list");
  const cmsContentEmpty = document.getElementById("cms-content-empty");
  const contentCreateTitle = document.getElementById("content-create-title");
  const contentCreateType = document.getElementById("content-create-type");
  const contentCreateAccess = document.getElementById("content-create-access");
  const contentCreateCategory = document.getElementById("content-create-category");
  const contentCreateDescription = document.getElementById("content-create-description");
  const contentCreateDuration = document.getElementById("content-create-duration");
  const contentCreateBody = document.getElementById("content-create-body");
  const contentCreateBodyLabel = document.getElementById("content-create-body-label");
  const contentCreateRecipeFields = document.getElementById("content-create-recipe-fields");
  const contentCreateIngredients = document.getElementById("content-create-ingredients");
  const contentCreateSteps = document.getElementById("content-create-steps");
  const contentCreateCoverFile = document.getElementById("content-create-cover-file");
  const contentCreateVideoFile = document.getElementById("content-create-video-file");
  const contentCreateAudioFile = document.getElementById("content-create-audio-file");
  const contentCreateVideoLabel = document.getElementById("content-create-video-label");
  const contentCreateAudioLabel = document.getElementById("content-create-audio-label");
  const contentCreateMessage = document.getElementById("content-create-message");
  const contentEditCard = document.getElementById("content-edit-card");
  const contentEditTitle = document.getElementById("content-edit-title");
  const contentEditCategory = document.getElementById("content-edit-category");
  const contentCreateTaxonomy = document.getElementById("content-create-taxonomy");
  const contentEditTaxonomy = document.getElementById("content-edit-taxonomy");
  const contentEditDescription = document.getElementById("content-edit-description");
  const contentEditDuration = document.getElementById("content-edit-duration");
  const contentEditOrder = document.getElementById("content-edit-order");
  const contentEditAccess = document.getElementById("content-edit-access");
  const contentEditMessage = document.getElementById("content-edit-message");
  const contentEditorState = document.getElementById("content-editor-state");
  const contentEditorTitle = document.getElementById("content-editor-title");
  const contentEditorStatus = document.getElementById("content-editor-status");
  const contentEditorMore = document.getElementById("content-editor-more");
  const contentEditorMenu = document.getElementById("content-editor-menu");
  const contentEditorActions = document.getElementById("content-editor-actions");
  const contentBottomSave = document.getElementById("content-bottom-save");
  const contentBottomPublish = document.getElementById("content-bottom-publish");
  const contentLivePreview = document.getElementById("content-live-preview");
  const contentStudioWorkspace = document.getElementById("content-studio-workspace");
  const contentStudioTabs = document.getElementById("content-studio-tabs");
  const contentUnsavedDialog = document.getElementById("content-unsaved-dialog");
  const contentMediaCard = document.getElementById("content-media-card");
  const contentCoverCurrent = document.getElementById("content-cover-current");
  const contentVideoCurrent = document.getElementById("content-video-current");
  const contentVideoControl = document.getElementById("content-video-control");
  const contentAudioControl = document.getElementById("content-audio-control");
  const contentAudioCurrent = document.getElementById("content-audio-current");
  const contentCoverFile = document.getElementById("content-cover-file");
  const contentVideoFile = document.getElementById("content-video-file");
  const contentAudioFile = document.getElementById("content-audio-file");
  const contentMediaConfirmation = document.getElementById("content-media-confirmation");
  const contentMediaPreview = document.getElementById("content-media-preview");
  const contentMediaSummary = document.getElementById("content-media-summary");
  const contentMediaMessage = document.getElementById("content-media-message");
  const contentMediaConfirm = document.getElementById("content-media-confirm");
  const contentMediaCancel = document.getElementById("content-media-cancel");
  const contentLifecycleCard = document.getElementById("content-lifecycle-card");
  const contentLifecycleTitle = document.getElementById("content-lifecycle-title");
  const contentLifecycleMessage = document.getElementById("content-lifecycle-message");
  const contentLifecyclePreview = document.getElementById("content-lifecycle-preview");
  const contentLifecyclePreviewButton = document.getElementById("content-lifecycle-preview-button");
  const contentLifecycleConfirm = document.getElementById("content-lifecycle-confirm");
  const contentLifecycleCancel = document.getElementById("content-lifecycle-cancel");
  const contentVersionHistory = document.getElementById("content-version-history");
  const contentVersionHistoryEmpty = document.getElementById("content-version-history-empty");
  const contentCreateRevision = document.getElementById("content-create-revision");
  const memberHomeLessons = document.getElementById("member-home-lessons");
  const memberFreeSection = document.getElementById("member-free-section");
  const memberFreeLesson = document.getElementById("member-free-lesson");
  const memberHomeEmpty = document.getElementById("member-home-empty");
  const memberHomeCategories = document.getElementById("member-home-categories");
  const memberTrainingList = document.getElementById("member-training-list");
  const memberTrainingEmpty = document.getElementById("member-training-empty");
  const memberLessonContent = document.getElementById("member-lesson-content");
  const memberLibrarySearch = document.getElementById("member-library-search");
  const memberLibraryChips = document.getElementById("member-library-chips");
  const memberScheduleContent = document.getElementById("member-schedule-content");
  const memberMeditationList = document.getElementById("member-meditation-list");
  const memberMeditationEmpty = document.getElementById("member-meditation-empty");
  const memberMeditationSearch = document.getElementById("member-meditation-search");
  const contentRecipeCard = document.getElementById("content-recipe-card");
  const contentNutritionCard = document.getElementById("content-nutrition-card");
  const contentNutritionBody = document.getElementById("content-nutrition-body");
  const contentNutritionMessage = document.getElementById("content-nutrition-message");
  const recipeIngredientsList = document.getElementById("recipe-ingredients-list");
  const recipeStepsList = document.getElementById("recipe-steps-list");
  const recipeEditMessage = document.getElementById("recipe-edit-message");
  const memberRecipeList = document.getElementById("member-recipe-list");
  const memberRecipeEmpty = document.getElementById("member-recipe-empty");
  const memberRecipeSearch = document.getElementById("member-recipe-search");
  const memberRecipeChips = document.getElementById("member-recipe-chips");
  const memberNutritionList = document.getElementById("member-nutrition-list");
  const memberNutritionEmpty = document.getElementById("member-nutrition-empty");
  const memberNutritionSearch = document.getElementById("member-nutrition-search");
  const metricNodes = document.querySelectorAll("[data-metric]");
  const statusLabels = {active: "Активен", active_grace: "Grace", expired: "Просрочен", inactive: "Нет доступа"};
  const typeLabels = {trial: "Trial", paid: "Платная", gift: "Подарок", manual: "Ручной", unknown: "Не определено"};
  let sessionToken = null;
  let realMemberMode = false;
  let memberEntitled = false;
  let usersCursor = null;
  let searchTimer = null;
  let usersRequestController = null;
  let usersListScrollPosition = 0;
  let subscriptionsCursor = null;
  let failedSubscriptionsCursor = null;
  let subscriptionsSearchTimer = null;
  let deliveriesCursor = null;
  let scheduleCursor = null;
  let adminClassesCursor = null;
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
  let contentMediaLocalType = null;
  let contentMediaServerUrl = null;
  let contentMediaAttachedCoverUrl = null;
  let contentMediaGeneration = 0;
  let contentLifecycleActionId = null;
  let contentLifecycleMode = null;
  let memberPreviewMode = false;
  let memberCoverGeneration = 0;
  let memberAudioGeneration = 0;
  let memberAudioElement = null;
  let memberAudioUrl = null;
  let memberVideoGeneration = 0;
  let memberVideoElement = null;
  let memberVideoUrl = null;
  const memberCoverUrls = new Map();
  const memberCoverPending = new Map();
  const memberCoverControllers = new Set();
  const memberCoverQueue = createBoundedTaskQueue(4);
  const MEMBER_COVER_CACHE_LIMIT = 64;
  let memberCoverObserver = null;
  let memberLibraryItems = [];
  let memberMeditationItems = [];
  let memberRecipeItems = [];
  let memberNutritionItems = [];
  let cmsContentStatus = "all";
  let cmsContentItems = [];
  let studioCoverUrls = [];
  let studioCoverObserver = null;
  let studioCoverGeneration = 0;
  let contentEditorDirty = false;
  let pendingContentNavigation = null;
  let memberRecipeCategory = "all";
  let memberDetailContentType = "lesson";
  let recipeIngredients = [];
  let recipeSteps = [];
  let cmsTaxonomy = [];
  let memberLibraryCategory = "all";
  let adminSearchTimer = null;
  let adminSearchGeneration = 0;
  let adminSearchController = null;
  let memberSearchTimer = null;
  let adminNotificationItems = [];
  let adminCurrentNotificationKeys = new Set();
  let adminNotificationUnknownUnreadCount = 0;
  let adminNotificationPaginationIncomplete = false;
  let adminNotificationDisplayLimit = MAX_NOTIFICATION_PANEL_ITEMS;
  let adminNotificationPageLimit = MAX_NOTIFICATION_PANEL_ITEMS;
  const adminNotificationReadState = new Set();
  const adminNotificationResolvedState = new Set();
  const adminNotificationArchivedState = new Set();
  const adminNotificationAckPending = new Set();
  let adminNotificationAckError = "";

  const text = (tag, value, className) => {
    const node = document.createElement(tag);
    node.textContent = value === null || value === undefined || value === "" ? "—" : String(value);
    if (className) node.className = className;
    return node;
  };
  const adminIconPaths = {
    home: ["M3 11.5 12 4l9 7.5", "M5.5 10v10h13V10", "M9.5 20v-6h5v6"],
    content: ["M6 3h9l3 3v15H6z", "M15 3v4h4", "M9 11h6M9 15h6"],
    users: ["M16 20v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2", "M9 10a4 4 0 1 0 0-8 4 4 0 0 0 0 8Z", "M22 20v-2a4 4 0 0 0-3-3.87M16 2.13a4 4 0 0 1 0 7.75"],
    subscriptions: ["M3 6h18v12H3z", "M3 10h18", "M7 15h4"],
    calendar: ["M4 5h16v16H4z", "M8 3v4M16 3v4M4 10h16"],
    gift: ["M3 9h18v12H3z", "M2 5h20v4H2z", "M12 5v16", "M12 5H8.5a2.5 2.5 0 1 1 3.5-2.3V5Zm0 0h3.5A2.5 2.5 0 1 0 12 2.7V5Z"],
    renewal: ["M12 9v4M12 17h.01", "M10.3 3.7 2.5 17.2h19L13.7 3.7a2 2 0 0 0-3.4 0Z"],
    settings: ["M12 15.5a3.5 3.5 0 1 0 0-7 3.5 3.5 0 0 0 0 7Z", "M19.4 15a1.7 1.7 0 0 0 .34 1.88l.06.06-2.12 2.12-.06-.06a1.7 1.7 0 0 0-1.88-.34 1.7 1.7 0 0 0-1.03 1.55V20h-3v-.09a1.7 1.7 0 0 0-1.03-1.55 1.7 1.7 0 0 0-1.88.34l-.06.06-2.12-2.12.06-.06A1.7 1.7 0 0 0 7 14.7a1.7 1.7 0 0 0-1.55-1.03H5v-3h.45A1.7 1.7 0 0 0 7 9.64a1.7 1.7 0 0 0-.34-1.88L6.6 7.7l2.12-2.12.06.06A1.7 1.7 0 0 0 10.66 6 1.7 1.7 0 0 0 11.7 4.45V4h3v.45A1.7 1.7 0 0 0 15.73 6a1.7 1.7 0 0 0 1.88-.34l.06-.06 2.12 2.12-.06.06a1.7 1.7 0 0 0-.34 1.88 1.7 1.7 0 0 0 1.55 1.03H21v3h-.06A1.7 1.7 0 0 0 19.4 15Z"],
    analytics: ["M4 20V10M10 20V4M16 20v-7M22 20V7"],
    bell: ["M18 8a6 6 0 0 0-12 0c0 7-3 7-3 9h18c0-2-3-2-3-9Z", "M10 21h4"],
    external: ["M14 3h7v7M10 14 21 3", "M21 14v6a1 1 0 0 1-1 1H4a1 1 0 0 1-1-1V4a1 1 0 0 1 1-1h6"],
    refresh: ["M20 6v6h-6", "M4 18v-6h6", "M6.5 8a7 7 0 0 1 11-2l2.5 6M17.5 16a7 7 0 0 1-11 2L4 12"],
    plus: ["M12 5v14M5 12h14"],
    search: ["M11 19a8 8 0 1 1 0-16 8 8 0 0 1 0 16ZM21 21l-4.35-4.35"],
    fullscreen: ["M8 3H3v5M16 3h5v5M8 21H3v-5M16 21h5v-5"],
    warning: ["M12 9v4M12 17h.01", "M10.3 3.7 2.5 17.2h19L13.7 3.7a2 2 0 0 0-3.4 0Z"],
    more: ["M5 12h.01M12 12h.01M19 12h.01"],
  };
  const installAdminIcons = () => document.querySelectorAll("[data-admin-icon]").forEach((button) => {
    const host = button.querySelector(":scope > span:first-child");
    if (!host || host.childNodes.length) return;
    const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    svg.setAttribute("viewBox", "0 0 24 24"); svg.setAttribute("fill", "none"); svg.setAttribute("stroke", "currentColor"); svg.setAttribute("stroke-width", "1.7"); svg.setAttribute("stroke-linecap", "round"); svg.setAttribute("stroke-linejoin", "round");
    (adminIconPaths[button.dataset.adminIcon] || []).forEach((value) => { const path=document.createElementNS("http://www.w3.org/2000/svg","path"); path.setAttribute("d",value); svg.append(path); });
    host.append(svg);
  });
  const memberIconPaths = {
    strength: ["M5 9v6M8 7v10M16 7v10M19 9v6M8 12h8"],
    flexibility: ["M12 5a2 2 0 1 0 0-4 2 2 0 0 0 0 4ZM12 6v6l-5 5M12 9l6 3M12 12l4 7"],
    glutes: ["M8 5c-2 3-3 6-2 10 1 3 3 5 6 5s5-2 6-5c1-4 0-7-2-10M12 6v14"],
    posture: ["M12 5a2 2 0 1 0 0-4 2 2 0 0 0 0 0 4ZM12 6v8M8 9h8M12 14l-3 7M12 14l3 7"],
    pelvic_floor: ["M7 7c1 2 2 3 5 3s4-1 5-3M6 11c2 5 10 5 12 0M9 16c1 2 5 2 6 0"],
    mobility: ["M7 7h10M17 7l-3-3M17 7l-3 3M17 17H7M7 17l3-3M7 17l3 3M12 9v6"],
    feet: ["M10 4c2 1 2 4 1 7s-1 7-4 7-3-3-2-5 2-10 6-9ZM16 7c2 2 3 6 2 10-1 3-3 4-5 3-2-2 0-5 1-7s0-7 2-6"],
    recovery: ["M5 18c8 0 13-5 14-13-8 1-13 6-14 13ZM6 17c3-4 6-7 11-10"],
    neck: ["M9 4v5c0 2-1 3-3 4M15 4v5c0 2 1 3 3 4M8 19c2-3 6-3 8 0"],
    back: ["M10 3c-2 4-2 7 0 10l-2 8M14 3c2 4 2 7 0 10l2 8M9 9h6"],
    lower_back: ["M7 6c3 2 7 2 10 0M7 18c3-2 7-2 10 0M8 9c2 2 6 2 8 0M8 15c2-2 6-2 8 0"],
    legs: ["M9 3l1 8-2 10M15 3l-1 8 2 10M10 11h4"],
    recipes: ["M5 11h14c0 5-3 8-7 8s-7-3-7-8ZM8 8c0-2 1-3 3-4M13 8c0-2 1-3 3-4"],
    nutrition: ["M6 3h12v18H6zM9 8h6M9 12h6M9 16h4"],
    calendar: ["M5 5h14v15H5zM8 2v6M16 2v6M5 10h14M9 14h2M13 14h2"],
    heart: ["M12 20 4 12C0 7 6 2 12 8c6-6 12-1 8 4Z"],
    completed: ["M5 12l4 4L19 6M12 22a10 10 0 1 1 9-6"],
    history: ["M4 5v6h6M5 11a8 8 0 1 0 2-6M12 7v5l4 2"],
    subscription: ["M4 6h16v12H4zM4 10h16M8 15h3"],
    settings: ["M12 15a3 3 0 1 0 0-6 3 3 0 0 0 0 6ZM12 2v3M12 19v3M2 12h3M19 12h3M5 5l2 2M17 17l2 2M19 5l-2 2M7 17l-2 2"],
    lotus: ["M12 20c-5-2-7-6-7-10 4 1 6 3 7 6 1-3 3-5 7-6 0 4-2 8-7 10ZM12 16c-3-3-3-7 0-12 3 5 3 9 0 12Z"],
  };
  const memberIcon = (name) => {
    const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    svg.setAttribute("viewBox", "0 0 24 24");
    svg.setAttribute("fill", "none");
    svg.setAttribute("stroke", "currentColor");
    svg.setAttribute("stroke-width", "1.6");
    svg.setAttribute("stroke-linecap", "round");
    svg.setAttribute("stroke-linejoin", "round");
    (memberIconPaths[name] || memberIconPaths.recovery).forEach((definition) => {
      const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
      path.setAttribute("d", definition);
      svg.append(path);
    });
    return svg;
  };
  const hydrateMemberIcons = () => {
    document.querySelectorAll("[data-member-icon]").forEach((container) => {
      container.replaceChildren(memberIcon(container.dataset.memberIcon));
    });
  };
  const api = (path, options = {}) => fetch(path, {
    method: "GET", headers: {Authorization: `Bearer ${sessionToken}`},
    cache: "no-store", credentials: "omit", signal: options.signal,
  }).then((response) => {
    if (response.status === 401) throw new Error("session_ended");
    if (response.status === 403) throw new Error("access_revoked");
    if (!response.ok) throw new Error("api_failed");
    return response.json();
  });
  const memberPath = (adminPath, memberPathValue) => realMemberMode ? memberPathValue : adminPath;
  const syncMemberAccess = (access) => {
    if (!realMemberMode || !access) return;
    memberEntitled = Boolean(access.has_active_access);
    document.getElementById("member-continue").textContent = memberEntitled
      ? "Выбрать тренировку"
      : "Получить доступ";
    const statusLabel = document.getElementById("member-profile-access");
    if (statusLabel) {
      statusLabel.textContent = memberEntitled
        ? "Доступ активен"
        : "Нет активного доступа";
    }
  };
  const showApiError = (error) => {
    if (error.message === "session_ended") status.textContent = "Сессия завершена. Закройте и снова откройте админ-платформу.";
    else if (error.message === "access_revoked") status.textContent = "У вас больше нет доступа к админ-платформе.";
    else status.textContent = "Не удалось загрузить данные. Попробуйте обновить.";
  };
  const memberCategoryLabel = (category) => {
    if (!category) return "Другое";
    const labels = {
      strength: "Сила", strength_training: "Сила", flexibility: "Гибкость",
      glutes: "Ягодицы", posture: "Осанка", pelvic_floor: "Тазовое дно",
      mobility: "Мобилити", feet: "Стопы", recovery: "Восстановление",
      main_workout: "Основные тренировки", warmup: "Зарядки",
    };
    if (labels[category]) return labels[category];
    return category.split("_").map((part) => part.charAt(0).toUpperCase() + part.slice(1)).join(" ");
  };
  const formatDuration = (seconds) => {
    if (!seconds) return "—";
    const value = Number(seconds);
    return `${Math.floor(value / 60)}:${String(value % 60).padStart(2, "0")}`;
  };
  const parseDuration = (value) => {
    const normalized = String(value || "").trim();
    if (!normalized) return null;
    if (!/^\d{1,3}:\d{2}$/.test(normalized)) throw new Error("invalid_duration");
    const [minutes, seconds] = normalized.split(":").map(Number);
    const total = minutes * 60 + seconds;
    if (seconds > 59 || total < 1 || total > 86400) throw new Error("invalid_duration");
    return total;
  };
  const contentErrorMessage = (error, item = currentCmsContent) => {
    if (error.message === "content_version_changed" || error.message === "content_publish_state_changed") return "Материал изменился. Обновите данные и повторите.";
    if (error.message === "content_publish_preview_expired") return "Предпросмотр устарел. Проверьте материал ещё раз.";
    if (error.message === "invalid_duration") return "Укажите длительность в формате ММ:СС, например 29:04.";
    if (error.message === "content_publish_incomplete") {
      if (item && item.content_type === "lesson") return "Добавьте длительность и видео тренировки.";
      if (item && item.content_type === "meditation") return "Добавьте длительность и аудио или видео.";
      if (item && item.content_type === "recipe") return "Добавьте хотя бы один ингредиент и один шаг.";
      if (item && item.content_type === "nutrition_material") return "Добавьте текст материала.";
    }
    return "Операция не выполнена. Проверьте данные и повторите.";
  };
  const appendInlineFormatting = (container, value) => {
    String(value).split(/(\*\*[^*]+\*\*)/).filter(Boolean).forEach((part) => {
      if (part.startsWith("**") && part.endsWith("**")) container.append(text("strong", part.slice(2, -2)));
      else container.append(document.createTextNode(part));
    });
  };
  const appendRestrictedText = (container, value) => {
    let list = null; let listType = null;
    String(value || "").split("\n").forEach((rawLine) => {
      const line = rawLine.trim();
      if (!line) { list = null; listType = null; return; }
      const ordered = /^\d+\.\s+/.test(line);
      const unordered = /^[-*]\s+/.test(line);
      if (ordered || unordered) {
        const type = ordered ? "ol" : "ul";
        if (!list || listType !== type) { list = document.createElement(type); listType = type; container.append(list); }
        const item = document.createElement("li"); appendInlineFormatting(item, line.replace(ordered ? /^\d+\.\s+/ : /^[-*]\s+/, "")); list.append(item); return;
      }
      list = null; listType = null;
      const element = document.createElement(line.startsWith("### ") ? "h3" : "p");
      appendInlineFormatting(element, line.replace(/^###\s+/, "")); container.append(element);
    });
  };
  const cancelMemberCoverWork = () => {
    memberCoverGeneration += 1;
    if (memberCoverObserver) memberCoverObserver.disconnect();
    memberCoverObserver = null;
    memberCoverQueue.clear();
    memberCoverControllers.forEach((controller)=>controller.abort());
    memberCoverControllers.clear();
    memberCoverPending.clear();
  };
  const clearMemberCoverUrls = () => {
    cancelMemberCoverWork();
    memberCoverUrls.forEach((url) => URL.revokeObjectURL(url));
    memberCoverUrls.clear();
  };
  const cacheMemberCover = (key, url) => {
    const old=memberCoverUrls.get(key);
    if(old && old !== url) URL.revokeObjectURL(old);
    memberCoverUrls.delete(key); memberCoverUrls.set(key,url);
    while(memberCoverUrls.size > MEMBER_COVER_CACHE_LIMIT){
      const [expiredKey,expiredUrl]=memberCoverUrls.entries().next().value;
      memberCoverUrls.delete(expiredKey); URL.revokeObjectURL(expiredUrl);
    }
  };
  const renderMemberCoverImage = (container,item,url) => {
    const image=document.createElement("img"); image.src=url; image.alt=`Обложка: ${item.title}`;
    container.replaceChildren(image);
  };
  const ensureMemberCoverObserver = () => {
    if(memberCoverObserver || !("IntersectionObserver" in window)) return memberCoverObserver;
    memberCoverObserver=new IntersectionObserver((entries,observer)=>entries.forEach((entry)=>{
      if(!entry.isIntersecting) return;
      observer.unobserve(entry.target); const load=entry.target._memberCoverLoad;
      if(load) load();
    }),{rootMargin:"300px 0px"});
    return memberCoverObserver;
  };
  const clearMemberAudio = () => {
    memberAudioGeneration += 1;
    if (memberAudioElement) {
      memberAudioElement.pause();
      memberAudioElement.removeAttribute("src");
      memberAudioElement.load();
      memberAudioElement = null;
    }
    if (memberAudioUrl) URL.revokeObjectURL(memberAudioUrl);
    memberAudioUrl = null;
  };
  const clearMemberVideo = () => {
    memberVideoGeneration += 1;
    if (memberVideoElement) {
      memberVideoElement.pause();
      memberVideoElement.removeAttribute("src");
      memberVideoElement.load();
    }
    memberVideoElement = null;
    if (memberVideoUrl) URL.revokeObjectURL(memberVideoUrl);
    memberVideoUrl = null;
  };
  const memberCover = (item, large = false) => {
    const generation = memberCoverGeneration;
    const key=String(item.cover_media_id || "");
    const container = document.createElement("div");
    container.className = large ? "member-cover member-cover-large" : "member-cover";
    if (!item.has_cover || !item.cover_media_id) {
      container.append(text("span", "Материал клуба", "member-cover-placeholder"));
      return container;
    }
    const cached=memberCoverUrls.get(key);
    if(cached){ memberCoverUrls.delete(key); memberCoverUrls.set(key,cached); renderMemberCoverImage(container,item,cached); return container; }
    container.append(text("span", "Загружаем обложку…", "member-cover-placeholder"));
    const load=()=>{
      const pending=getOrCreateCachedResource({cache:memberCoverUrls,pending:memberCoverPending,key,store:cacheMemberCover,load:()=>{
        const controller=new AbortController(); memberCoverControllers.add(controller);
        return memberCoverQueue.add(()=>fetch(
          memberPath(`/api/admin/content/cms/${encodeURIComponent(item.content_id)}/media/${encodeURIComponent(item.cover_media_id)}`, `/api/member/content/${encodeURIComponent(item.content_id)}/media/${encodeURIComponent(item.cover_media_id)}`),
          {headers:{Authorization:`Bearer ${sessionToken}`},cache:"no-store",credentials:"omit",signal:controller.signal}
        ).then((response)=>{ if(!response.ok) throw new Error("cover_unavailable"); return response.blob(); })
          .then((blob)=>URL.createObjectURL(blob))
          .finally(()=>memberCoverControllers.delete(controller)));
      }});
      pending.then((url)=>{ if(generation===memberCoverGeneration && container.isConnected) renderMemberCoverImage(container,item,url); }).catch((error)=>{
        if(generation===memberCoverGeneration && error.name!=="AbortError" && container.isConnected) container.replaceChildren(text("span","Обложка недоступна","member-cover-placeholder"));
      });
    };
    container._memberCoverLoad=load;
    const observer=ensureMemberCoverObserver();
    if(observer) observer.observe(container); else load();
    return container;
  };
  const memberContentCard = (item) => {
    const article = document.createElement("article");
    article.className = "member-card member-content-card member-lesson-card";
    const button = document.createElement("button");
    button.type = "button";
    button.append(memberCover(item));
    const copy = document.createElement("div");
    copy.className = "member-lesson-copy";
    const meta = document.createElement("div");
    meta.className = "member-lesson-meta";
    meta.append(text("span", (item.categories || []).map((entry) => entry.title).join(" · ") || memberCategoryLabel(item.category)));
    if (item.access_level === "free") meta.append(text("span", "Бесплатно", "member-free-badge"));
    if (!realMemberMode) {
      const statusBadge = text("span", ({draft: "Черновик", published: "Опубликовано", archived: "Архив"}[item.status] || item.status), `member-preview-badge member-status-${item.status}`);
      meta.append(statusBadge);
      if ((item.content_type === "lesson" && !item.has_video) || (item.content_type === "meditation" && !item.has_audio)) {
        meta.append(text("span", item.content_type === "meditation" ? "Не добавлено аудио" : "Не добавлено видео", "member-media-missing"));
      }
    }
    const bookmark = text("span", "☆");
    bookmark.setAttribute("aria-hidden", "true");
    meta.append(bookmark);
    copy.append(meta, text("h2", item.title));
    if (item.duration_seconds) meta.append(text("span", `${Math.ceil(item.duration_seconds / 60)} мин.`, "member-duration-badge"));
    if (item.description) copy.append(text("p", item.description, "member-description-excerpt"));
    if (item.access_level === "free") copy.append(text("span", "Смотреть урок", "member-free-cta"));
    button.append(copy);
    button.addEventListener("click", () => loadMemberLesson(item.content_id, item.content_type).catch(showApiError));
    article.append(button);
    if (!realMemberMode) {
      const actions = document.createElement("div");
      actions.className = "member-admin-card-actions";
      const edit = text("button", item.status === "published" ? "Новая версия" : item.status === "archived" ? "Открыть в админке" : "Редактировать");
      edit.type = "button";
      edit.addEventListener("click", () => {
        if (item.status === "published") loadCmsContentDetails(item.content_id).then(createContentRevision).catch(showApiError);
        else loadCmsContentDetails(item.content_id).catch(showApiError);
      });
      const preview = text("button", "Предпросмотр", "secondary");
      preview.type = "button";
      preview.addEventListener("click", () => loadMemberLesson(item.content_id, item.content_type).catch(showApiError));
      actions.append(edit, preview);
      article.append(actions);
    }
    return article;
  };
  const showMemberScreen = (name) => {
    if (name !== "member-lesson") clearMemberAudio();
    if (name !== "member-lesson") clearMemberVideo();
    memberPreviewMode = true;
    document.body.classList.add("member-preview-mode");
    adminHero.hidden = true;
    bottomNav.hidden = true;
    memberBottomNav.hidden = false;
    memberShellHeader.hidden = false;
    document.getElementById("member-mode-label").textContent = realMemberMode ? "Закрытый клуб" : "Режим администратора";
    document.querySelectorAll(".member-admin-create").forEach((button) => { button.hidden = realMemberMode; });
    showScreen(name);
    const navigationName = (
      name === "member-lesson" || name === "member-meditations"
      || name === "member-recipes" || name === "member-nutrition"
    ) ? "member-library" : name;
    document.querySelectorAll("[data-member-nav]").forEach((button) => {
      button.classList.toggle("active", button.dataset.memberNav === navigationName);
    });
  };
  const loadMemberHome = () => {
    return api(memberPath("/api/admin/member-preview/home", "/api/member/home")).then((data) => {
      syncMemberAccess(data.access);
      cancelMemberCoverWork();
      memberHomeLessons.replaceChildren();
      const regularLessons = data.latest_lessons.filter((item) => item.access_level !== "free");
      regularLessons.forEach((item) => memberHomeLessons.append(memberContentCard(item)));
      memberHomeEmpty.hidden = regularLessons.length !== 0;
      memberFreeLesson.replaceChildren();
      const freeLesson = (data.free_lessons || []).find((item) => item.content_type === "lesson");
      memberFreeSection.hidden = !freeLesson;
      if (freeLesson) memberFreeLesson.append(memberContentCard(freeLesson));
      memberHomeCategories.replaceChildren();
      data.categories.forEach((entry) => {
        const card = document.createElement("article");
        card.className = "member-category-card";
        card.append(
          text("strong", entry.title || memberCategoryLabel(entry.slug || entry.category)),
          text("span", `${entry.count} ${entry.count === 1 ? "материал" : "материалов"}`)
        );
        memberHomeCategories.append(card);
      });
      showMemberScreen("member-home");
    });
  };
  const renderMemberLibrary = () => {
    const query = memberLibrarySearch.value.trim().toLocaleLowerCase("ru");
    const items = memberLibraryItems.filter((item) => {
      const categoryMatches = memberLibraryCategory === "all"
        || (item.categories || []).some((entry) => entry.slug === memberLibraryCategory);
      return categoryMatches && (!query || item.title.toLocaleLowerCase("ru").includes(query));
    });
    cancelMemberCoverWork();
    memberTrainingList.replaceChildren();
    items.forEach((item) => memberTrainingList.append(memberContentCard(item)));
    memberTrainingEmpty.hidden = items.length !== 0;
  };
  const configureMemberLibraryFilters = () => {
    const categoryMap = new Map(); memberLibraryItems.flatMap((item) => item.categories || []).forEach((item) => categoryMap.set(item.slug,item.title));
    memberLibraryChips.replaceChildren();
    [{key: "all", label: "Все"}, ...[...categoryMap].map(([key,label]) => ({key,label}))]
      .forEach((entry) => {
        const chip = text("button", entry.label, "member-chip");
        chip.type = "button";
        chip.dataset.memberCategory = entry.key;
        chip.classList.toggle("active", entry.key === memberLibraryCategory);
        chip.addEventListener("click", () => {
          memberLibraryCategory = entry.key;
          memberLibraryChips.querySelectorAll("button").forEach((button) => {
            button.classList.toggle("active", button.dataset.memberCategory === entry.key);
          });
          renderMemberLibrary();
        });
        memberLibraryChips.append(chip);
      });
  };
  const loadMemberLibrary = (category = "all") => {
    return api(memberPath("/api/admin/member-preview/content?limit=50", "/api/member/content?content_type=lesson&limit=50")).then((data) => {
      syncMemberAccess(data.access);
      memberLibraryItems = data.items;
      memberLibraryCategory = memberLibraryItems.some(
        (item) => (item.categories || []).some((entry) => entry.slug === category)
      ) ? category : "all";
      configureMemberLibraryFilters();
      renderMemberLibrary();
      showMemberScreen("member-library");
    });
  };
  const loadMemberLesson = (contentId, contentType = "lesson") => {
    return api(memberPath(`/api/admin/member-preview/content/${encodeURIComponent(contentId)}?content_type=${encodeURIComponent(contentType)}`, `/api/member/content/${encodeURIComponent(contentId)}`)).then((item) => {
      cancelMemberCoverWork();
      clearMemberAudio();
      clearMemberVideo();
      memberDetailContentType = contentType;
      memberLessonContent.replaceChildren();
      memberLessonContent.append(memberCover(item, true));
      const heading = document.createElement("header");
      const meta = document.createElement("div");
      meta.className = "member-lesson-meta";
      meta.append(text("span", (item.categories || []).map((entry) => entry.title).join(" · ") || memberCategoryLabel(item.category)));
      if (!realMemberMode) meta.append(text("span", ({draft: "Черновик", published: "Опубликовано", archived: "Архив"}[item.status] || item.status), `member-preview-badge member-status-${item.status}`));
      if (item.access_level === "free") meta.append(text("span", "Бесплатный урок", "member-free-badge"));
      heading.append(meta, text("h1", item.title));
      if (item.duration_seconds) heading.append(text(
        "p",
        item.content_type === "recipe"
          ? `Время приготовления: ${Math.ceil(item.duration_seconds / 60)} минут`
          : `${Math.ceil(item.duration_seconds / 60)} минут`
      ));
      memberLessonContent.append(heading);
      if (!realMemberMode) {
        const toolbar = document.createElement("section");
        toolbar.className = "member-card member-admin-toolbar";
        const edit = text("button", item.status === "published" ? "Создать новую версию" : "Открыть в админке");
        edit.type = "button";
        edit.addEventListener("click", () => loadCmsContentDetails(item.content_id).then(() => item.status === "published" ? createContentRevision() : null).catch(showApiError));
        toolbar.append(edit);
        if (item.status === "draft" || item.status === "published") {
          const lifecycle = text("button", item.status === "published" ? "Архивировать" : "Опубликовать", "secondary");
          lifecycle.type = "button";
          lifecycle.addEventListener("click", () => loadCmsContentDetails(item.content_id).then(previewContentLifecycle).catch(showApiError));
          toolbar.append(lifecycle);
        }
        memberLessonContent.append(toolbar);
      }
      if (item.description) {
        const description = document.createElement("section");
        description.className = "member-description formatted-content";
        appendRestrictedText(description, item.description);
        memberLessonContent.append(description);
      }
      if (realMemberMode && item.locked) {
        const locked=document.createElement("section"); locked.className="member-card member-locked-content";
        locked.append(text("h2","Доступно участникам клуба"),text("p","Получите доступ, чтобы открыть полный материал и медиаплеер."));
        const action=text("button","Получить доступ","member-button"); action.type="button"; action.addEventListener("click",()=>webApp.close()); locked.append(action); memberLessonContent.append(locked);
      }
      if (item.content_type === "recipe" && !item.locked) {
        const ingredients = document.createElement("section");
        ingredients.className = "member-recipe-detail";
        ingredients.append(text("h2", "Ингредиенты"));
        const ingredientList = document.createElement("ul");
        (item.ingredients || []).forEach((entry) => {
          ingredientList.append(text("li", entry.amount ? `${entry.name} — ${entry.amount}` : entry.name));
        });
        if (!item.ingredients || !item.ingredients.length) ingredientList.append(text("li", "Ингредиенты пока не добавлены"));
        ingredients.append(ingredientList);
        const preparation = document.createElement("section");
        preparation.className = "member-recipe-detail";
        preparation.append(text("h2", "Способ приготовления"));
        const stepList = document.createElement("ol");
        (item.steps || []).forEach((entry) => stepList.append(text("li", entry.instruction)));
        if (!item.steps || !item.steps.length) stepList.append(text("li", "Шаги пока не добавлены"));
        preparation.append(stepList);
        memberLessonContent.append(ingredients, preparation);
      }
      if (item.content_type === "nutrition_material" && !item.locked) {
        const body = document.createElement("section");
        body.className = "member-card member-nutrition-body";
        (item.body || "").split(/\n{2,}/).filter((paragraph) => paragraph.trim()).forEach((paragraph) => {
          body.append(text("p", paragraph));
        });
        memberLessonContent.append(body);
      }
      if (item.content_type === "meditation" && item.has_audio && item.audio_media_id && !item.locked) {
        const generation = memberAudioGeneration;
        const player = document.createElement("section");
        player.className = "member-card member-audio-player";
        const toggle = text("button", "▶"); toggle.type = "button"; toggle.disabled = true;
        const progress = document.createElement("input"); progress.type = "range"; progress.min = "0"; progress.max = "0"; progress.value = "0"; progress.step = "0.1"; progress.setAttribute("aria-label", "Позиция аудио");
        const times = document.createElement("div"); times.className = "member-audio-time";
        const current = text("span", "0:00"); const total = text("span", "—"); times.append(current, total);
        player.append(toggle, progress, times); memberLessonContent.append(player);
        fetch(memberPath(`/api/admin/content/cms/${encodeURIComponent(item.content_id)}/media/${encodeURIComponent(item.audio_media_id)}/audio`, `/api/member/content/${encodeURIComponent(item.content_id)}/media/${encodeURIComponent(item.audio_media_id)}`), {
          headers: {Authorization: `Bearer ${sessionToken}`}, cache: "no-store", credentials: "omit",
        }).then((response) => { if (!response.ok) throw new Error("audio_unavailable"); return response.blob(); }).then((blob) => {
          if (generation !== memberAudioGeneration) return;
          memberAudioUrl = URL.createObjectURL(blob);
          const audio = document.createElement("audio"); audio.preload = "metadata"; audio.src = memberAudioUrl;
          memberAudioElement = audio;
          const clock = (seconds) => Number.isFinite(seconds) ? `${Math.floor(seconds / 60)}:${String(Math.floor(seconds % 60)).padStart(2, "0")}` : "—";
          audio.addEventListener("loadedmetadata", () => { progress.max = String(audio.duration || 0); total.textContent = clock(audio.duration); toggle.disabled = false; });
          audio.addEventListener("timeupdate", () => { progress.value = String(audio.currentTime); current.textContent = clock(audio.currentTime); });
          audio.addEventListener("ended", () => { toggle.textContent = "▶"; });
          toggle.addEventListener("click", () => { if (audio.paused) audio.play().then(() => { toggle.textContent = "❚❚"; }).catch(() => null); else { audio.pause(); toggle.textContent = "▶"; } });
          progress.addEventListener("input", () => { audio.currentTime = Number(progress.value); });
        }).catch(() => { if (generation === memberAudioGeneration) player.replaceChildren(text("p", "Аудио временно недоступно.")); });
      }
      if (realMemberMode && !item.locked && item.content_type === "lesson" && item.has_video && item.video_media_id) {
        const generation = memberVideoGeneration;
        const player = document.createElement("section");
        player.className = "member-card member-video-shell member-video-loading";
        player.append(text("strong", "Загружаем видео…"), text("p", "Урок откроется после безопасной проверки доступа."));
        memberLessonContent.append(player);
        fetch(`/api/member/content/${encodeURIComponent(item.content_id)}/media/${encodeURIComponent(item.video_media_id)}`, {
          headers: {Authorization: `Bearer ${sessionToken}`}, cache: "no-store", credentials: "omit",
        }).then(async (response) => {
          if (!response.ok) {
            const payload = await response.json().catch(() => ({}));
            const error = new Error(payload.error || "video_unavailable");
            error.category = payload.error;
            error.status = response.status;
            throw error;
          }
          return response.blob();
        }).then((blob) => {
          if (generation !== memberVideoGeneration) return;
          if (blob.type !== "video/mp4") throw new Error("video_unavailable");
          memberVideoUrl = URL.createObjectURL(blob);
          const video = document.createElement("video");
          video.className = "member-video-player";
          video.controls = true;
          video.playsInline = true;
          video.preload = "metadata";
          video.src = memberVideoUrl;
          memberVideoElement = video;
          player.classList.remove("member-video-loading");
          player.replaceChildren(video);
        }).catch((error) => {
          if (generation !== memberVideoGeneration) return;
          if (error.status === 403 || error.category === "active_access_required") {
            memberEntitled = false;
            player.className = "member-card member-locked-content";
            player.replaceChildren(text("h2", "Доступ завершён"), text("p", "Обновите доступ, чтобы продолжить просмотр урока."));
            return;
          }
          player.classList.remove("member-video-loading");
          player.replaceChildren(text("strong", "Видео временно недоступно"), text("p", "Попробуйте открыть урок ещё раз позже."));
        });
      } else if (!item.locked && item.content_type !== "nutrition_material" && !(item.content_type === "meditation" && item.has_audio)) {
        const video = document.createElement("section");
        video.className = "member-video-shell";
        video.append(text("span", "▶", "member-play-icon"));
        video.append(text("strong", item.has_video ? "Видео загружено" : "Видео появится позже"));
        video.append(text("p", item.has_video ? "Воспроизведение доступно в режиме участника." : "К этому материалу видео пока не добавлено."));
        memberLessonContent.append(video);
      }
      showMemberScreen("member-lesson");
    });
  };
  const renderMemberMeditations = () => {
    const query = memberMeditationSearch.value.trim().toLocaleLowerCase("ru");
    const items = memberMeditationItems.filter((item) => !query || item.title.toLocaleLowerCase("ru").includes(query));
    cancelMemberCoverWork();
    memberMeditationList.replaceChildren();
    items.forEach((item) => memberMeditationList.append(memberContentCard(item)));
    memberMeditationEmpty.hidden = items.length !== 0;
  };
  const loadMemberMeditations = () => api(memberPath("/api/admin/member-preview/content?content_type=meditation&limit=50", "/api/member/content?content_type=meditation&limit=50")).then((data) => {
    syncMemberAccess(data.access);
    memberMeditationItems = data.items;
    renderMemberMeditations();
    showMemberScreen("member-meditations");
  });
  const renderMemberRecipes = () => {
    const query = memberRecipeSearch.value.trim().toLocaleLowerCase("ru");
    const items = memberRecipeItems.filter((item) => {
      const categoryMatches = memberRecipeCategory === "all" || (item.categories || []).some((entry) => entry.slug === memberRecipeCategory);
      return categoryMatches && (!query || item.title.toLocaleLowerCase("ru").includes(query));
    });
    cancelMemberCoverWork();
    memberRecipeList.replaceChildren();
    items.forEach((item) => memberRecipeList.append(memberContentCard(item)));
    memberRecipeEmpty.hidden = items.length !== 0;
  };
  const configureMemberRecipeFilters = () => {
    const categoryMap = new Map(); memberRecipeItems.flatMap((item) => item.categories || []).forEach((item) => categoryMap.set(item.slug,item.title));
    memberRecipeChips.replaceChildren();
    [{key: "all", label: "Все рецепты"}, ...[...categoryMap].map(([key,label]) => ({key,label}))].forEach((entry) => {
      const chip = text("button", entry.label, "member-chip");
      chip.type = "button";
      chip.classList.toggle("active", entry.key === memberRecipeCategory);
      chip.addEventListener("click", () => {
        memberRecipeCategory = entry.key;
        configureMemberRecipeFilters();
        renderMemberRecipes();
      });
      memberRecipeChips.append(chip);
    });
  };
  const loadMemberRecipes = () => api(memberPath("/api/admin/member-preview/content?content_type=recipe&limit=50", "/api/member/content?content_type=recipe&limit=50")).then((data) => {
    syncMemberAccess(data.access);
    memberRecipeItems = data.items;
    if (!memberRecipeItems.some((item) => (item.categories || []).some((entry) => entry.slug === memberRecipeCategory))) memberRecipeCategory = "all";
    configureMemberRecipeFilters();
    renderMemberRecipes();
    showMemberScreen("member-recipes");
  });
  const renderMemberNutrition = () => {
    const query = memberNutritionSearch.value.trim().toLocaleLowerCase("ru");
    const items = memberNutritionItems.filter((item) => !query || item.title.toLocaleLowerCase("ru").includes(query));
    cancelMemberCoverWork();
    memberNutritionList.replaceChildren();
    items.forEach((item) => memberNutritionList.append(memberContentCard(item)));
    memberNutritionEmpty.hidden = items.length !== 0;
  };
  const loadMemberNutrition = () => api(memberPath("/api/admin/member-preview/content?content_type=nutrition_material&limit=50", "/api/member/content?content_type=nutrition_material&limit=50")).then((data) => {
    syncMemberAccess(data.access);
    memberNutritionItems = data.items;
    renderMemberNutrition();
    showMemberScreen("member-nutrition");
  });
  const renderMemberScheduleEmpty = () => {
    const empty = document.createElement("article");
    empty.className = "member-card member-empty-state member-empty-art member-schedule-empty";
    const icon = text("span", "");
    icon.append(memberIcon("calendar"));
    empty.append(icon, text("h2", "Расписание клуба"), text("p", "Информация о ближайших встречах появится здесь."));
    memberScheduleContent.replaceChildren(empty);
  };
  const memberBookingCard = (item) => {
    const card=document.createElement("article"); card.className="member-class-row";
    card.append(text("strong",item.title),text("small",new Date(item.starts_at).toLocaleString("ru-RU",{dateStyle:"medium",timeStyle:"short"})),text("span",item.viewer_booking_status || item.status,"badge"));
    if(item.zoom_url){ const join=text("a","Открыть Zoom","member-button"); join.href=item.zoom_url; join.rel="noopener noreferrer"; card.append(join); }
    else if(item.viewer_booking_status === "checkout_open" && item.checkout_url){ const pay=text("a","Продолжить оплату","member-button"); pay.href=item.checkout_url; pay.rel="noopener noreferrer"; card.append(pay); }
    return card;
  };
  const loadMemberProfile = () => {
    showMemberScreen("member-profile");
    if(!realMemberMode) return Promise.resolve();
    return Promise.all([api("/api/member/me"),api("/api/member/classes")]).then(([profile,bookings])=>{
      syncMemberAccess(profile.access);
      document.getElementById("member-profile-name").textContent=profile.profile.first_name || (profile.profile.username ? `@${profile.profile.username}` : "Профиль");
      const upcoming=document.getElementById("member-upcoming-classes"); const history=document.getElementById("member-class-history");
      upcoming.replaceChildren(...((bookings.upcoming || []).map(memberBookingCard))); history.replaceChildren(...((bookings.history || []).map(memberBookingCard)));
      if(!upcoming.children.length) upcoming.append(text("p","Предстоящих занятий нет.","member-empty"));
      if(!history.children.length) history.append(text("p","Истории пока нет.","member-empty"));
    });
  };
  const loadMemberSchedule = () => {
    if (realMemberMode) {
      return api("/api/member/schedule").then((data) => {
        clearScheduleImages();
        memberScheduleContent.replaceChildren();
        (data.classes || []).forEach((item) => {
          const card=document.createElement("article"); card.className="member-card member-class-card";
          card.append(text("p",new Date(item.starts_at).toLocaleString("ru-RU",{dateStyle:"long",timeStyle:"short"}),"member-kicker"),text("h2",item.title),text("p",item.description || "Онлайн-занятие в Zoom"));
          const meta=document.createElement("div"); meta.className="badges"; meta.append(text("span",`${item.duration_minutes} мин`,"badge"),text("span",`€${(item.price_amount/100).toFixed(2)}`,"badge"),text("span",`${item.paid_bookings}/${item.capacity} мест`,"badge")); card.append(meta);
          if(item.zoom_url){ const join=text("a","Открыть Zoom","member-button"); join.href=item.zoom_url; join.rel="noopener noreferrer"; card.append(join); }
          else if(item.viewer_booking_status === "paid") card.append(text("p",item.status === "confirmed" ? "Вы записаны. Ссылка появится после подтверждения занятия." : "Вы записаны. Ожидаем подтверждения группы.","hint"));
          else if(item.viewer_booking_status === "checkout_open") { const book=text("button","Продолжить оплату","member-button"); book.type="button"; book.addEventListener("click",()=>writeMemberJson("POST",`/api/member/classes/${encodeURIComponent(item.class_id)}/book`,{}).then((result)=>{ if(result.checkout_url) webApp.openLink(result.checkout_url); }).catch(showApiError)); card.append(book); }
          else { card.append(text("p",`Запись оплачивается сейчас. При отмене или недоборе ${item.minimum_participants} участников платёж будет возвращён.`,"hint")); const full=Number(item.paid_bookings)>=Number(item.capacity); const closed=new Date(item.booking_deadline)<=new Date(); const book=text("button",full ? "Мест нет" : closed ? "Запись закрыта" : "Записаться","member-button"); book.type="button"; book.disabled=full || closed; book.addEventListener("click",()=>writeMemberJson("POST",`/api/member/classes/${encodeURIComponent(item.class_id)}/book`,{}).then((result)=>{ if(result.checkout_url) webApp.openLink(result.checkout_url); }).catch(showApiError)); card.append(book); }
          memberScheduleContent.append(card);
        });
        if (!(data.classes || []).length) renderMemberScheduleEmpty();
        if (data.has_schedule && !(data.classes || []).length) {
          memberScheduleContent.querySelector("p").textContent =
            "Расписание на текущий месяц доступно в основном меню бота.";
        }
        showMemberScreen("member-schedule");
      });
    }
    const today = moscowDate();
    const params = new URLSearchParams({from: today, to: today, status: "all", limit: "1"});
    return api(`/api/admin/schedule?${params.toString()}`).then((data) => {
      clearScheduleImages();
      if (!data.items.length) {
        renderMemberScheduleEmpty();
      } else {
        const schedule = data.items[0];
        const card = document.createElement("article");
        card.className = "member-card member-monthly-schedule";
        const heading = document.createElement("header");
        const icon = text("span", "", "member-monthly-schedule-icon");
        icon.append(memberIcon("calendar"));
        heading.append(icon, text("div", ""));
        heading.lastElementChild.append(text("p", schedule.period_label, "member-kicker"), text("h2", schedule.title));
        const preview = scheduleImageContainer(schedule, scheduleImageGeneration, true);
        preview.classList.add("member-schedule-preview");
        card.append(heading, preview, text("p", `Обновлено: ${formatDate(schedule.updated_at)}`, "member-schedule-updated"));
        memberScheduleContent.replaceChildren(card);
      }
      showMemberScreen("member-schedule");
    }).catch((error) => {
      renderMemberScheduleEmpty();
      showMemberScreen("member-schedule");
      if (error.message === "session_ended" || error.message === "access_revoked") showApiError(error);
    });
  };
  const exitMemberPreview = () => {
    if (realMemberMode) { webApp.close(); return Promise.resolve(); }
    memberPreviewMode = false;
    cancelMemberCoverWork();
    clearMemberAudio();
    document.body.classList.remove("member-preview-mode");
    memberBottomNav.hidden = true;
    memberShellHeader.hidden = true;
    adminHero.hidden = false;
    bottomNav.hidden = false;
    const destination = adminScreenBeforeClub;
    if (destination === "content") return loadContent();
    if (destination === "users") return loadUsers();
    if (destination === "subscriptions") return loadSubscriptions();
    if (destination === "schedule") return loadSchedule();
    if (destination === "gifts") return loadGifts();
    if (destination === "failed-subscriptions") return loadFailedSubscriptions(false);
    if (destination === "system") return loadSystem();
    showScreen(destination === "more" ? "more" : "overview");
    return destination === "overview" ? loadDashboard() : Promise.resolve();
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
    contentMediaGeneration += 1;
    if (contentMediaLocalUrl) URL.revokeObjectURL(contentMediaLocalUrl);
    if (contentMediaServerUrl) URL.revokeObjectURL(contentMediaServerUrl);
    if (contentMediaAttachedCoverUrl) URL.revokeObjectURL(contentMediaAttachedCoverUrl);
    contentMediaLocalUrl = null;
    contentMediaLocalType = null;
    contentMediaServerUrl = null;
    contentMediaAttachedCoverUrl = null;
  };
  const clearStudioCoverLoads = () => {
    studioCoverGeneration += 1;
    if (studioCoverObserver) studioCoverObserver.disconnect();
    studioCoverObserver = null;
    studioCoverUrls.forEach((url) => URL.revokeObjectURL(url));
    studioCoverUrls = [];
  };
  const resetContentMediaDraft = () => {
    if (contentMediaLocalUrl) URL.revokeObjectURL(contentMediaLocalUrl);
    if (contentMediaServerUrl) URL.revokeObjectURL(contentMediaServerUrl);
    contentMediaLocalUrl = null;
    contentMediaLocalType = null;
    contentMediaServerUrl = null;
    contentMediaUploadId = null;
    contentCoverFile.value = "";
    contentVideoFile.value = "";
    contentAudioFile.value = "";
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
  let activeAdminScreen = "overview";
  let adminScreenBeforeClub = "overview";
  const adminRootScreen = (name) => ({
    "user-details": "users", "subscription-details": "subscriptions",
    "failed-subscription-details": "failed-subscriptions",
    "schedule-details": "schedule", "schedule-upload": "schedule",
    "gift-details": "gifts", "delivery-details": "system",
    "content-details": "content", "content-create": "content",
  }[name] || name);
  const syncAdminShellActions = (name) => {
    const root = adminRootScreen(name);
    document.getElementById("topbar-create-content").hidden = root !== "overview";
  };
  const showScreen = (name) => {
    if (name !== "schedule" && name !== "schedule-details" && name !== "member-schedule" && scheduleImageUrls.size) {
      clearScheduleImages();
    }
    if (name !== "schedule-upload" && (scheduleUploadLocalUrl || scheduleUploadServerUrl)) {
      clearScheduleUploadUrls();
    }
    if (name !== "content-details") {
      clearContentMediaUrls();
      contentMediaUploadId = null;
    }
    document.querySelectorAll("[data-screen]").forEach((node) => { node.hidden = !adminScreenIsVisible(name, node.dataset.screen); });
    if (!name.startsWith("member-")) {
      activeAdminScreen = adminRootScreen(name);
      document.body.dataset.adminScreen = activeAdminScreen;
      syncAdminShellActions(name);
    }
    document.querySelectorAll("[data-nav]").forEach((node) => { node.classList.toggle("active", node.dataset.nav === adminRootScreen(name)); });
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
  const writeMemberJson = (method, path, body) => fetch(path, {
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
  const dashboardEmpty = (container, label) => container.replaceChildren(text("p", label, "overview-empty"));
  const dashboardRow = (title, meta, badge) => {
    const row = document.createElement("div"); row.className = "overview-row";
    const copy = document.createElement("div"); copy.append(text("strong", title), text("small", meta || "—"));
    row.append(copy); if (badge) row.append(text("span", badge, "overview-status"));
    return row;
  };
  const renderDashboardContent = (data) => {
    dashboardContentList.replaceChildren();
    const published = data.items.filter((item) => item.status === "published").length;
    document.getElementById("dashboard-published-count").textContent = `${data.items.length >= 50 ? "≥" : ""}${published}`;
    if (!data.items.length) return dashboardEmpty(dashboardContentList, "Материалов пока нет.");
    data.items.slice(0, 4).forEach((item) => {
      const button = document.createElement("button"); button.type = "button"; button.className = "overview-content-card";
      button.append(text("span", ({lesson:"Урок",meditation:"Медитация",recipe:"Рецепт",nutrition_material:"Питание"}[item.content_type] || item.content_type), "overview-type"), text("strong", item.title), text("small", ({draft:"Черновик",published:"Опубликован",archived:"Архив"}[item.status] || item.status)));
      button.addEventListener("click", () => loadCmsContentDetails(item.content_id)); dashboardContentList.append(button);
    });
  };
  const renderDashboardUsers = (data) => {
    dashboardUsersList.replaceChildren();
    if (!data.items.length) return dashboardEmpty(dashboardUsersList, "Пользователей пока нет.");
    data.items.slice(0, 4).forEach((user) => {
      const button = document.createElement("button"); button.type = "button"; button.className = "overview-user-row";
      button.append(text("strong", user.username ? `@${user.username}` : "Без username"), text("small", String(user.telegram_id)), text("span", statusLabels[user.access_status] || user.access_status, "overview-status"), text("small", user.expiry_date ? new Date(user.expiry_date).toLocaleDateString("ru-RU") : "—"));
      button.addEventListener("click", () => loadUserDetails(user.telegram_id)); dashboardUsersList.append(button);
    });
  };
  const renderDashboardSchedules = (data) => {
    dashboardScheduleList.replaceChildren();
    document.getElementById("dashboard-schedule-count").textContent = String(data.summary && data.summary.total_future != null ? data.summary.total_future : data.items.length);
    if (!data.items.length) return dashboardEmpty(dashboardScheduleList, "Ближайших расписаний нет.");
    data.items.slice(0, 3).forEach((item) => dashboardScheduleList.append(dashboardRow(item.title, item.period_label, item.status === "upcoming" ? "Опубликовано" : null)));
  };
  const renderDashboardGifts = (data) => {
    dashboardGiftsList.replaceChildren();
    document.querySelector(".admin-gifts-overview").classList.toggle("no-actionable", !data.items.some((gift) => gift.requires_attention));
    if (!data.items.length) return dashboardEmpty(dashboardGiftsList, "Подарков пока нет.");
    data.items.slice(0, 4).forEach((gift) => dashboardGiftsList.append(dashboardRow(`Подарок на ${gift.duration_label}`, gift.recipient && gift.recipient.username ? `@${gift.recipient.username}` : gift.public_reference, gift.status_label)));
  };
  const renderDashboardFailures = (data) => {
    dashboardFailedList.replaceChildren();
    document.querySelector(".admin-failed-overview").classList.toggle("no-attention", !data.items.length);
    if (!data.items.length) return dashboardEmpty(dashboardFailedList, "Проблем продления нет.");
    data.items.slice(0, 4).forEach((item) => dashboardFailedList.append(dashboardRow(item.username ? `@${item.username}` : (item.first_name || `ID ${item.telegram_id}`), `${item.reason_label} · попыток ${item.attempt_count}`, failedStatusLabels[item.status] || item.status)));
  };
  const setAttentionCount = (count) => {
    ["dashboard-attention-count","topbar-attention","sidebar-attention","more-attention"].forEach((id)=>{
      const node=document.getElementById(id); node.textContent=String(count); node.hidden=count===0;
    });
    document.getElementById("dashboard-open-attention").hidden=count===0;
  };
  const notificationPageUrl = (path,cursor) => `${path}&cursor=${encodeURIComponent(cursor)}`;
  const refreshAttentionCount = () => Promise.all([
    api("/api/admin/failed-subscriptions?state=attention&limit=50"),
    api("/api/admin/gifts?status=review_required&duration=all&limit=50"),
    api("/api/admin/deliveries?status=permanently_failed&limit=50"),
    api("/api/admin/system"),
  ]).then(([failed,gifts,deliveries,system]) => {
    const systemItems=buildAdminNotificationItems([],[],[],system);
    return Promise.all([
      collectAdminNotificationKeys({firstPage:failed,aggregate:failed.summary.attention,keyFor:(item)=>`failed:${item.operation_id}`,fetchNext:(cursor)=>api(notificationPageUrl("/api/admin/failed-subscriptions?state=attention&limit=50",cursor))}),
      collectAdminNotificationKeys({firstPage:gifts,aggregate:gifts.summary.requires_attention,keyFor:(item)=>`gift:${item.gift_id}`,fetchNext:(cursor)=>api(notificationPageUrl("/api/admin/gifts?status=review_required&duration=all&limit=50",cursor))}),
      collectAdminNotificationKeys({firstPage:deliveries,aggregate:system.deliveries.permanently_failed,keyFor:(item)=>`delivery:${item.delivery_id}`,fetchNext:(cursor)=>api(notificationPageUrl("/api/admin/deliveries?status=permanently_failed&limit=50",cursor))}),
    ]).then((results)=>{
      adminCurrentNotificationKeys=new Set(systemItems.map((item)=>item.key));
      results.forEach((result)=>result.keys.forEach((key)=>adminCurrentNotificationKeys.add(key)));
      adminNotificationUnknownUnreadCount=results.reduce((count,result)=>count+adminNotificationUnknownUnread(result),0);
      adminNotificationPaginationIncomplete=results.some((result)=>!result.complete);
      adminNotificationItems=orderAdminNotificationItems([
        ...buildAdminNotificationItems(results[0].items,results[1].items,results[2].items,{scheduler:{},removals:{}}),
        ...systemItems,
      ]);
      renderAdminNotificationPanel(); renderNotificationCenter(); updateAdminNotificationBadge();
      return {failed,gifts,deliveries,system};
    });
  });
  const loadDashboard = () => {
    status.textContent = "Загружаем данные…";
    const today = new Date();
    document.getElementById("dashboard-clock").textContent = today.toLocaleString("ru-RU", {weekday:"long",day:"numeric",month:"long",hour:"2-digit",minute:"2-digit"});
    return api("/api/admin/dashboard").then((data) => {
      metricNodes.forEach((node) => { node.textContent = String(valueAtPath(data, node.dataset.metric) ?? "—"); });
      document.getElementById("dashboard-migration-count").textContent = String(data.system.migrations.count);
      document.getElementById("dashboard-scheduler-count").textContent = String(data.system.scheduler.known_jobs);
      document.getElementById("dashboard-job-errors").textContent = String(data.system.scheduler.failed_last_24h);
      showScreen("overview");
      refresh.hidden = false;
      status.textContent = "Доступ подтверждён";
      dashboardContentList.replaceChildren();
      dashboardUsersList.replaceChildren();
      dashboardScheduleList.replaceChildren();
      dashboardGiftsList.replaceChildren();
      dashboardFailedList.replaceChildren();
      dashboardEmpty(dashboardContentList, "Материалы загружаются при открытии Контента.");
      dashboardEmpty(dashboardUsersList, "Участники загружаются при открытии раздела.");
      dashboardEmpty(dashboardScheduleList, "Расписание загружается при открытии раздела.");
      dashboardEmpty(dashboardGiftsList, "Подарки загружаются при открытии раздела.");
      dashboardEmpty(dashboardFailedList, "Уведомления загружаются отдельно.");
      return data;
    });
  };
  const userPrimaryStatus = (user) => {
    if (user.payment_failed && user.access_status === "active_grace") return {label:"Grace",tone:"warning"};
    if (user.payment_failed) return {label:"Проблема оплаты",tone:"danger"};
    if (user.access_type === "trial" && user.access_status === "active") return {label:"Пробный",tone:"info"};
    if (user.access_status === "active") return {label:"В клубе",tone:"success"};
    if (user.access_status === "expired") return {label:"Нет доступа",tone:"muted"};
    return {label:"Нет доступа",tone:"muted"};
  };
  const userAccessSummary = (user) => {
    if (user.expiry_date) return `${user.auto_renew ? "Продлевается" : "Доступ"} · до ${formatDate(user.expiry_date)}`;
    if (user.access_type === "unknown") return "История доступа отсутствует";
    return typeLabels[user.access_type] || "Доступ не определён";
  };
  const addBadges = (container, user) => {
    const badges = document.createElement("div");
    badges.className = "badges";
    const primary=userPrimaryStatus(user);
    badges.append(text("span", primary.label, `badge user-status-badge ${primary.tone}`));
    container.append(badges);
  };
  const userCard = (user) => {
    const article = document.createElement("article");
    article.className = "card user-card";
    const button = document.createElement("button");
    button.type = "button";
    const identityCell = document.createElement("div"); identityCell.className = "user-identity-cell";
    const identityCopy=document.createElement("div"); identityCopy.append(text("h2",user.first_name || (user.username ? `@${user.username}` : "Участник")),text("small",user.username ? `@${user.username}` : `ID ${user.telegram_id}`));
    identityCell.append(text("span", (user.first_name || user.username || "?").slice(0, 1).toUpperCase(), "user-avatar"),identityCopy);
    const access = document.createElement("div"); access.className = "user-access-cell"; addBadges(access, user);
    const subscription = text("p", userAccessSummary(user), "user-subscription-cell");
    const expiry = text("p", user.expiry_date ? formatDate(user.expiry_date) : "—", "user-expiry-cell");
    identityCopy.append(text("small", `ID ${user.telegram_id}`, "user-telegram-cell"));
    const activity = text("p", "", "user-activity-cell");
    const lastVisit = text("p", "", "user-last-visit-cell");
    const action = text("span", "›", "user-action-cell");
    button.append(identityCell, access, subscription, expiry, activity, lastVisit, action);
    button.addEventListener("click", () => loadUserDetails(user.telegram_id));
    article.append(button);
    return article;
  };
  const loadUsers = (append = false) => {
    status.textContent = "Загружаем пользователей…";
    const params = new URLSearchParams({limit: "25", status: usersStatus.value});
    if (usersSearch.value.trim()) params.set("q", usersSearch.value.trim());
    if (append && usersCursor) params.set("cursor", usersCursor);
    if (!append && usersRequestController) usersRequestController.abort();
    const controller = new AbortController();
    if (!append) usersRequestController = controller;
    return api(`/api/admin/users?${params.toString()}`, {signal:controller.signal}).then((data) => {
      if (!append && usersRequestController !== controller) return data;
      if (!append) usersList.replaceChildren();
      data.items.forEach((user) => usersList.append(userCard(user)));
      usersCursor = data.next_cursor;
      usersMore.hidden = !data.has_more;
      showScreen("users");
      status.textContent = `Пользователей показано: ${usersList.children.length}`;
      return data;
    }).catch((error) => {
      if (error && error.name === "AbortError") return null;
      throw error;
    }).finally(() => {
      if (usersRequestController === controller) usersRequestController = null;
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
  const userSectionCard = (section, title, pairs) => {
    const card = detailCard(title, pairs);
    card.dataset.userSection = section;
    return card;
  };
  const emptyUserSection = (section, message) => {
    const article = document.createElement("article");
    article.className = "card empty-state";
    article.dataset.userSection = section;
    article.append(text("p", message));
    return article;
  };
  const selectUserProfileTab = (section) => {
    document.querySelectorAll("[data-user-tab]").forEach((tab) => {
      const selected = tab.dataset.userTab === section;
      tab.classList.toggle("active", selected);
      tab.setAttribute("aria-selected", selected ? "true" : "false");
    });
    detailsContent.querySelectorAll("[data-user-section]").forEach((node) => {
      node.hidden = node.dataset.userSection !== section;
    });
    manualAccessCard.hidden = section !== "club" || !manualAccessUserId;
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
  function loadUserDetails(userId, initialTab = "overview") {
    status.textContent = "Загружаем профиль…";
    return api(`/api/admin/users/${encodeURIComponent(userId)}`).then((user) => {
      usersListScrollPosition = window.scrollY;
      detailsContent.replaceChildren();
      userProfileHeader.replaceChildren();
      const displayName=[user.first_name,user.last_name].filter(Boolean).join(" ") || (user.username ? `@${user.username}` : "Участник");
      const profile=document.createElement("article"); profile.className="participant-profile-hero";
      profile.append(text("span",displayName.slice(0,1).toUpperCase(),"participant-avatar"));
      const profileCopy=document.createElement("div"); profileCopy.className="participant-profile-copy"; profileCopy.append(text("h1",displayName),text("p",user.username ? `@${user.username}` : `ID ${user.telegram_id}`));
      const profileBadges=document.createElement("div"); profileBadges.className="participant-profile-badges"; addBadges(profileBadges,user); profileBadges.append(text("span",typeLabels[user.access_type] || "Доступ не определён","badge access-type-badge"));
      profileCopy.append(profileBadges,text("small",user.expiry_date ? `Доступ до ${formatDate(user.expiry_date)}` : "Действующий доступ не найден"));
      profile.append(profileCopy);
      userProfileHeader.append(profile);
      const overview=document.createElement("section"); overview.className="user-overview-section"; overview.dataset.userSection="overview";
      const metrics=document.createElement("div"); metrics.className="user-profile-metrics";
      const accessMetric=document.createElement("article"); accessMetric.className="user-profile-metric"; accessMetric.append(text("small","Текущий доступ"),text("strong",typeLabels[user.access_type] || "Не определён"),text("span",user.expiry_date ? `до ${formatDate(user.expiry_date)}` : "Без активного срока"));
      const state=userPrimaryStatus(user); const statusMetric=document.createElement("article"); statusMetric.className="user-profile-metric"; statusMetric.append(text("small","Статус"),text("strong",state.label),text("span",user.auto_renew ? "Автопродление включено" : "Без автопродления"));
      metrics.append(accessMetric,statusMetric); overview.append(metrics);
      const activity=document.createElement("section"); activity.className="user-recent-activity"; activity.append(text("h2","Последняя активность"));
      if(user.access_history.length){ const list=document.createElement("div"); list.className="user-activity-list"; user.access_history.slice(0,4).forEach((event)=>{ const row=document.createElement("div"); row.append(text("span","↻","user-activity-icon"),text("strong",event.event_type.replaceAll("_"," ")),text("small",`${event.source} · ${formatDate(event.created_at)}`)); list.append(row); }); activity.append(list); }
      else activity.append(text("p","Авторитетных событий доступа пока нет.","hint"));
      overview.append(activity);
      detailsContent.append(
        overview,
        userSectionCard("club", "Клуб", [["Статус", statusLabels[user.access_status]], ["Источник доступа", typeLabels[user.access_type]], ["Доступ до", user.expiry_date], ["Автопродление", user.auto_renew ? "Включено" : "Выключено"], ["Trial использован", user.trial_used ? "Да" : "Нет"]]),
        userSectionCard("payments", "Платежи", [["Оплаченный доступ", user.paid ? "Да" : "Нет"], ["Ошибка оплаты", user.payment_failed ? "Да" : "Нет"], ["Grace до", user.grace_period_end], ["Customer", user.stripe.customer_id], ["Subscription", user.stripe.subscription_id]]),
        emptyUserSection("gifts", "Связанные подарки отсутствуют в текущих данных профиля."),
        emptyUserSection("classes", "Связанные занятия отсутствуют в текущих данных профиля."),
        emptyUserSection("activity", "Авторитетные данные активности пока не собираются."),
        userSectionCard("diagnostics", "Диагностика удаления", user.removal ? [["Статус", user.removal.status], ["Причина", user.removal.reason], ["Access expiry", user.removal.access_expiry], ["Обновлено", user.removal.updated_at]] : [["Статус", "Нет операции"]]),
        userSectionCard("history", "История доступа", user.access_history.length ? user.access_history.map((event) => [event.event_type, `${event.source}: ${event.old_expiry || "—"} → ${event.new_expiry || "—"}`]) : [["События", "Нет"]])
      );
      configureManualAccess(user);
      selectUserProfileTab(initialTab);
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
    button.addEventListener("click", () => loadUserDetails(subscription.telegram_id, "club"));
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

  const failedStatusLabels = {
    pending: "Ожидает", processing: "В обработке", stripe_cancelled: "Stripe отменена",
    collection_stopped: "Invoice закрыт", telegram_failed: "Ошибка Telegram",
    telegram_removed: "Удалён из Telegram", retryable_failed: "Нужен повтор",
    manual_review: "Ручная проверка", completed: "Завершена", superseded: "Замещена",
    unknown: "Неизвестно",
  };
  const failedSubscriptionCard = (operation) => {
    const article = document.createElement("article");
    article.className = `card user-card${operation.needs_attention ? " attention" : ""}`;
    const button = document.createElement("button"); button.type = "button";
    button.append(text("h2", operation.username ? `@${operation.username}` : (operation.first_name || `ID ${operation.telegram_id}`)));
    button.append(text("p", operation.reason_label));
    button.append(text("p", `Обновлено: ${formatDate(operation.updated_at)} · Попыток: ${operation.attempt_count}`));
    const badges = document.createElement("div"); badges.className = "badges";
    badges.append(text("span", failedStatusLabels[operation.status] || "Неизвестно", "badge"));
    if (operation.stale) badges.append(text("span", "Stale", "badge attention-label"));
    if (operation.needs_attention) badges.append(text("span", "⚠ Требует внимания", "badge attention-label"));
    button.append(badges); button.addEventListener("click", () => loadFailedSubscriptionDetails(operation.operation_id));
    article.append(button); return article;
  };
  const loadFailedSubscriptions = (append = false) => {
    const params = new URLSearchParams({limit:"25", state:failedSubscriptionsFilter.value});
    if (append && failedSubscriptionsCursor) params.set("cursor", failedSubscriptionsCursor);
    return api(`/api/admin/failed-subscriptions?${params.toString()}`).then((data) => {
      if (!append) failedSubscriptionsList.replaceChildren();
      data.items.forEach((item) => failedSubscriptionsList.append(failedSubscriptionCard(item)));
      failedSubscriptionsCursor = data.next_cursor; failedSubscriptionsMore.hidden = !data.has_more;
      failedSubscriptionsEmpty.hidden = failedSubscriptionsList.children.length !== 0;
      failedSubscriptionMetricNodes.forEach((node) => { node.textContent = String(data.summary[node.dataset.failedMetric] ?? "—"); });
      showScreen("failed-subscriptions"); status.textContent = "Проблемы продления";
    }).catch(showApiError);
  };
  const retryFailedSubscription = (operation) => postAdminJson(
    `/api/admin/failed-subscriptions/${encodeURIComponent(operation.operation_id)}/retry-preview`, {}
  ).then((preview) => {
    if (!window.confirm(`Повторить безопасную обработку операции для Telegram ID ${preview.telegram_id}?`)) {
      return postAdminJson(`/api/admin/failed-subscriptions/${encodeURIComponent(operation.operation_id)}/retry-cancel`, {action_id:preview.action_id});
    }
    return postAdminJson(`/api/admin/failed-subscriptions/${encodeURIComponent(operation.operation_id)}/retry-confirm`, {action_id:preview.action_id}).then((result) => {
      status.textContent = `Результат: ${result.lifecycle_result}`;
      return loadFailedSubscriptionDetails(operation.operation_id);
    });
  }).catch(showApiError);
  function loadFailedSubscriptionDetails(operationId) {
    return api(`/api/admin/failed-subscriptions/${encodeURIComponent(operationId)}`).then((operation) => {
      failedSubscriptionDetailsContent.replaceChildren();
      failedSubscriptionDetailsContent.append(
        detailCard("Пользователь", [["Telegram ID",operation.telegram_id],["Username",operation.username ? `@${operation.username}`:"—"],["Имя",operation.first_name||"—"]]),
        detailCard("Состояние", [["Статус",failedStatusLabels[operation.status]||"Неизвестно"],["Фаза",operation.current_phase],["Причина",operation.reason_label],["Ошибка",operation.last_error_category||"—"],["Попытки",operation.attempt_count]]),
        detailCard("Billing references", [["Subscription",operation.stripe.subscription_id||"—"],["Invoice",operation.stripe.failed_invoice_id||"—"]]),
        detailCard("Прогресс фаз", [["Stripe cancellation",formatDate(operation.stripe_cancelled_at)],["Invoice closed",formatDate(operation.collection_stopped_at)],["Telegram ban",formatDate(operation.telegram_banned_at)],["Telegram removed",formatDate(operation.telegram_removed_at)],["DB finalized",formatDate(operation.db_finalized_at)]]),
        detailCard("Timing", [["Access expiry",formatDate(operation.access_expiry)],["Lease",formatDate(operation.lease_until)],["Завершено",formatDate(operation.completed_at)],["Generation",operation.claim_generation],["Создано",formatDate(operation.created_at)],["Обновлено",formatDate(operation.updated_at)]])
      );
      if (operation.retry_allowed) { const retry=text("button","Повторить сейчас"); retry.type="button"; retry.addEventListener("click",()=>retryFailedSubscription(operation)); failedSubscriptionDetailsContent.append(retry); }
      showScreen("failed-subscription-details"); status.textContent="Операция продления";
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
          node.textContent = data.database.connection_errors ? "Ошибка" : "Работает";
        } else if (node.dataset.systemMetric === "schema") {
          node.textContent = data.migrations.latest ? "Актуальна" : "Требует проверки";
        } else {
          node.textContent = String(valueAtPath(data, node.dataset.systemMetric) ?? "—");
        }
      });
      replaceDefinitionList(systemAttention, [
        ["Не доставлено окончательно", data.deliveries.permanently_failed],
        ["Ошибки планировщика за 24 часа", data.scheduler.failed_last_24h],
        ["Повторные удаления", data.removals.retryable],
      ]);
      replaceDefinitionList(systemDeliveryMetrics, [
        ["Ожидают", data.deliveries.pending], ["В обработке", data.deliveries.processing],
        ["С ошибкой", data.deliveries.failed], ["Не доставлено окончательно", data.deliveries.permanently_failed],
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
  const attentionRow = (kind, title, description, severity, actionLabel, action) => {
    const row=document.createElement("article"); row.className="card attention-row";
    const copy=document.createElement("div"); copy.append(text("small",kind),text("strong",title),text("p",description || "Требуется безопасная проверка."));
    const badge=text("span",severity,severity === "Критично" ? "badge attention-label" : "badge");
    const button=text("button",actionLabel,"secondary"); button.type="button"; button.addEventListener("click",action);
    row.append(copy,badge,button); return row;
  };
  const loadAttention = () => {
    status.textContent="Загружаем очередь внимания…";
    return Promise.all([
      api("/api/admin/failed-subscriptions?state=attention&limit=25"),
      api("/api/admin/deliveries?status=permanently_failed&limit=25"),
      api("/api/admin/gifts?status=review_required&duration=all&limit=25"),
    ]).then(([failed,deliveries,gifts]) => {
      attentionList.replaceChildren();
      failed.items.forEach((item)=>attentionList.append(attentionRow("Подписка",item.username ? `@${item.username}` : item.first_name || "Операция подписки",item.reason_label,"Критично","Открыть",()=>loadFailedSubscriptionDetails(item.operation_id))));
      deliveries.items.forEach((item)=>attentionList.append(attentionRow("Доставка",item.delivery_label,item.explanation,"Критично","Открыть",()=>loadDeliveryDetails(item.delivery_id))));
      gifts.items.forEach((item)=>attentionList.append(attentionRow("Подарок",item.public_reference,item.status_label,"Проверка","Открыть",()=>loadGiftDetails(item.gift_id))));
      if(!attentionList.children.length) attentionList.append(text("p","Сейчас нет нерешённых событий.","card"));
      const count=failed.items.length+deliveries.items.length+gifts.items.length;
      refreshAttentionCount().catch(()=>null);
      showScreen("attention"); status.textContent=`Требуют внимания: ${count}`;
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
    return Promise.all([api(`/api/admin/schedule?${scheduleParams(append).toString()}`),api("/api/admin/classes?limit=50")]).then(([data,classes]) => {
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
      renderAdminClasses(classes.items || [],false,classes);
      showScreen("schedule");
      status.textContent = data.items.length
        ? (archive ? "Архив расписаний" : "Расписание клуба")
        : scheduleEmpty.textContent;
    });
  };
  const renderAdminClasses = (items,append=false,page={}) => {
    if(!append) classCalendarList.replaceChildren();
    classCalendarList.querySelector(".admin-classes-more")?.remove();
    items.forEach((item)=>{
      const row=document.createElement("article"); row.className="class-calendar-row";
      const copy=document.createElement("div"); copy.append(text("strong",item.title),text("small",`${new Date(item.starts_at).toLocaleString("ru-RU")} · ${item.duration_minutes} мин · ${item.paid_bookings}/${item.capacity} оплачено`));
      const state=text("span",({draft:"Черновик",open:"Открыта запись",confirmed:"Подтверждено",cancelled:"Отменено",completed:"Завершено"}[item.status] || item.status),`badge class-${item.status}`); row.append(copy,state);
      if(["draft","open"].includes(item.status)){ const edit=text("button","Редактировать","secondary"); edit.type="button"; edit.addEventListener("click",()=>{ editingClassId=item.class_id; classCreateForm.hidden=false; document.getElementById("class-title").value=item.title; document.getElementById("class-description").value=item.description || ""; document.getElementById("class-start").value=new Date(new Date(item.starts_at).getTime()-new Date(item.starts_at).getTimezoneOffset()*60000).toISOString().slice(0,16); document.getElementById("class-duration").value=String(item.duration_minutes); document.getElementById("class-zoom-url").value=item.zoom_url; document.getElementById("class-price").value=(item.price_amount/100).toFixed(2); document.getElementById("class-capacity").value=String(item.capacity); document.getElementById("class-minimum").value=String(item.minimum_participants); document.getElementById("class-deadline").value=new Date(new Date(item.booking_deadline).getTime()-new Date(item.booking_deadline).getTimezoneOffset()*60000).toISOString().slice(0,16); document.getElementById("class-create-message").textContent="Редактирование доступно до появления оплаченных бронирований."; classCreateForm.scrollIntoView({behavior:"smooth",block:"start"}); }); row.append(edit); }
      if(item.status === "draft"){ const open=text("button","Открыть запись","secondary"); open.type="button"; open.addEventListener("click",()=>writeAdminJson("PATCH",`/api/admin/classes/${encodeURIComponent(item.class_id)}/status`,{status:"open"}).then(()=>loadSchedule(false)).catch(showApiError)); row.append(open); }
      if(item.status === "open"){ const unpublish=text("button","В черновик","secondary"); unpublish.type="button"; unpublish.addEventListener("click",()=>writeAdminJson("PATCH",`/api/admin/classes/${encodeURIComponent(item.class_id)}/status`,{status:"draft"}).then(()=>loadSchedule(false)).catch(showApiError)); row.append(unpublish); }
      if(item.status === "confirmed"){ const complete=text("button","Завершить","secondary"); complete.type="button"; complete.addEventListener("click",()=>writeAdminJson("PATCH",`/api/admin/classes/${encodeURIComponent(item.class_id)}/status`,{status:"completed"}).then(()=>loadSchedule(false)).catch(showApiError)); row.append(complete); }
      const attendees=text("button","Записавшиеся","secondary"); attendees.type="button"; attendees.addEventListener("click",()=>api(`/api/admin/classes/${encodeURIComponent(item.class_id)}/bookings`).then((data)=>{ const list=document.createElement("div"); list.className="class-booking-list"; if(!data.items.length) list.append(text("p","Записей пока нет.","hint")); data.items.forEach((booking)=>list.append(detailCard(booking.first_name || (booking.username ? `@${booking.username}` : `ID ${booking.telegram_id}`),[["Статус оплаты",booking.status],["Сумма",`${(booking.amount/100).toFixed(2)} ${booking.currency.toUpperCase()}`],["Создано",formatDate(booking.created_at)]]))); row.append(list); attendees.disabled=true; }).catch(showApiError)); row.append(attendees);
      if(["draft","open","confirmed"].includes(item.status)){ const cancel=text("button","Отменить","secondary"); cancel.type="button"; cancel.addEventListener("click",()=>writeAdminJson("PATCH",`/api/admin/classes/${encodeURIComponent(item.class_id)}/status`,{status:"cancelled"}).then(()=>loadSchedule(false)).catch(showApiError)); row.append(cancel); }
      classCalendarList.append(row);
    });
    adminClassesCursor=page.next_cursor || null;
    if(!items.length && !append) classCalendarList.append(text("p","Создайте первое бронируемое Zoom-занятие.","hint"));
    if(page.has_more && adminClassesCursor){
      const more=text("button","Показать ещё занятия","secondary admin-classes-more"); more.type="button";
      more.addEventListener("click",()=>{
        more.disabled=true;
        api(`/api/admin/classes?limit=50&cursor=${encodeURIComponent(adminClassesCursor)}`).then((next)=>renderAdminClasses(next.items || [],true,next)).catch(showApiError);
      });
      classCalendarList.append(more);
    }
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
    clearStudioCoverLoads();
    const params = new URLSearchParams({category: "all"});
    if (contentSearch.value.trim()) params.set("q", contentSearch.value.trim());
    return Promise.all([
      api(`/api/admin/content?${params.toString()}`),
      api(`/api/admin/content/cms?status=${encodeURIComponent(cmsContentStatus)}&limit=50`),
    ]).then(([data, cms]) => {
      cmsContentItems = cms.items || [];
      contentList.replaceChildren();
      data.items.forEach((item) => contentList.append(contentCard(item)));
      contentEmpty.hidden = data.items.length !== 0;
      const selectedCategory = contentCategory.value;
      const categories = new Map();
      cms.items.forEach((item) => (item.categories || []).forEach((entry) => categories.set(entry.id, entry.title)));
      const effectiveCategory = contentStudioEffectiveCategory(cms.items, selectedCategory);
      contentCategory.replaceChildren();
      const allCategories = document.createElement("option"); allCategories.value = "all"; allCategories.textContent = "Все категории"; contentCategory.append(allCategories);
      [...categories.entries()].sort((left, right) => left[1].localeCompare(right[1], "ru")).forEach(([value, label]) => {
        const option = document.createElement("option"); option.value = value; option.textContent = label; contentCategory.append(option);
      });
      contentCategory.value = effectiveCategory;
      const query = contentSearch.value.trim().toLocaleLowerCase("ru");
      const items = cms.items.filter((item) => {
        const matchesType = contentType.value === "all" || item.content_type === contentType.value;
        const matchesCategory = effectiveCategory === "all" || (item.categories || []).some((entry) => entry.id === effectiveCategory);
        const matchesQuery = !query || item.title.toLocaleLowerCase("ru").includes(query);
        return matchesType && matchesCategory && matchesQuery;
      });
      cmsContentList.replaceChildren();
      items.forEach((item) => cmsContentList.append(cmsContentCard(item)));
      cmsContentEmpty.hidden = items.length !== 0;
      showScreen("content");
      status.textContent = `Материалов: ${items.length}`;
    });
  };
  const renderEditorLibrary = () => {
    const host=document.getElementById("content-editor-library-list"); host.replaceChildren();
    cmsContentItems.forEach((item)=>{
      const button=document.createElement("button"); button.type="button"; button.className=`studio-editor-library-item${currentCmsContent && currentCmsContent.content_id === item.content_id ? " active" : ""}`;
      button.append(text("strong",item.title),text("small",`${cmsTypeLabel(item.content_type)} · ${cmsStatusLabel(item.status)}`));
      button.addEventListener("click",()=>guardContentNavigation(()=>loadCmsContentDetails(item.content_id)));
      host.append(button);
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

  const cmsTypeLabel = (contentType) => ({lesson: "Урок", meditation: "Медитация", recipe: "Рецепт", nutrition_material: "Материал нутрициолога"}[contentType] || contentType);
  const cmsStatusLabel = (value) => ({draft: "Черновик", published: "Опубликовано", archived: "Архив"}[value] || value);
  const setContentEditorDirty = (dirty = true) => {
    contentEditorDirty = dirty;
    contentEditorState.textContent = dirty ? "Есть несохранённые изменения" : "Все изменения сохранены ✓";
    contentEditorState.classList.toggle("dirty", dirty);
    if (contentBottomSave) contentBottomSave.disabled = !dirty;
    if (contentBottomPublish) contentBottomPublish.disabled = dirty || !currentCmsContent || currentCmsContent.status !== "draft";
  };
  const runPendingContentNavigation = () => {
    const action = pendingContentNavigation;
    pendingContentNavigation = null;
    contentUnsavedDialog.close();
    if (action) action();
  };
  const guardContentNavigation = (action) => {
    if (!contentEditorDirty) { action(); return; }
    pendingContentNavigation = action;
    contentUnsavedDialog.showModal();
  };
  const renderContentLivePreview = (item = currentCmsContent) => {
    contentLivePreview.replaceChildren(text("p", "Предпросмотр участника", "eyebrow"));
    if (!item) {
      contentLivePreview.append(text("h2", "Выберите материал"), text("p", "Здесь появится безопасный preview.", "hint"));
      return;
    }
    const title = item.status === "draft" ? contentEditTitle.value.trim() || item.title : item.title;
    const description = item.status === "draft" ? contentEditDescription.value.trim() : item.description;
    const duration = item.status === "draft" ? contentEditDuration.value.trim() : (item.duration_seconds ? formatDuration(item.duration_seconds) : "");
    const mediaTypes = new Set((item.media || []).map((entry) => entry.media_type));
    const cover = document.createElement("div"); cover.className = "studio-preview-cover";
    const coverUrl = contentStudioCoverUrl({
      localUrl: contentMediaLocalUrl,
      localMediaType: contentMediaLocalType,
      attachedUrl: contentMediaAttachedCoverUrl,
    });
    if (coverUrl) {
      const image = document.createElement("img"); image.src = coverUrl; image.alt = `Обложка ${title}`; cover.append(image);
    } else cover.append(text("span", mediaTypes.has("cover") ? "Обложка материала" : cmsTypeLabel(item.content_type)));
    contentLivePreview.append(cover, text("span", cmsStatusLabel(item.status), `badge studio-status ${item.status}`), text("h2", title));
    const previewAccess = item.status === "draft" ? contentEditAccess.value : item.access_level;
    if (previewAccess === "free") contentLivePreview.insertBefore(text("span", "Бесплатно", "badge member-free-badge"), contentLivePreview.querySelector("h2"));
    if (duration) contentLivePreview.append(text("p", duration, "studio-preview-duration"));
    const categories = item.status === "draft"
      ? [...contentEditTaxonomy.querySelectorAll("input[type=checkbox]:checked")].map((input) => input.closest("label").textContent.trim())
      : (item.categories || []).map((entry) => entry.title);
    if (categories.length) contentLivePreview.append(text("p", categories.join(" · "), "studio-preview-categories"));
    if (description) appendRestrictedText(contentLivePreview, description);
    else contentLivePreview.append(text("p", "Описание пока не заполнено.", "hint"));
    const mediaLabel = item.content_type === "lesson" ? (mediaTypes.has("video") ? "Видео прикреплено" : "Видео ещё не добавлено")
      : item.content_type === "meditation" ? (mediaTypes.has("audio") || mediaTypes.has("video") ? "Практика готова" : "Аудио или видео ещё не добавлено") : null;
    if (mediaLabel) contentLivePreview.append(text("p", mediaLabel, "studio-preview-media"));
  };
  const selectedTaxonomyIds = (fieldset) => [...fieldset.querySelectorAll("input[type=checkbox]:checked")].map((input) => input.value);
  const renderTaxonomy = (fieldset, categories, selected = []) => {
    const host = fieldset.querySelector(".taxonomy-options"); host.replaceChildren(); fieldset.hidden = categories.length === 0;
    const groups = new Map(); categories.forEach((item) => { const key=item.group || "other"; if(!groups.has(key)) groups.set(key,[]); groups.get(key).push(item); });
    groups.forEach((items, group) => { const section=document.createElement("div"); section.className="taxonomy-group"; section.append(text("strong", group === "first_aid" ? "Фитнес-аптечка" : group === "workout" ? "Основные направления" : "Категории")); items.forEach((item) => { const label=document.createElement("label"); label.className="taxonomy-option"; const input=document.createElement("input"); input.type="checkbox"; input.value=item.id; input.checked=selected.includes(item.id); label.append(input,text("span",item.title)); section.append(label); }); host.append(section); });
  };
  const loadTaxonomy = (contentType, fieldset, selected=[]) => api(`/api/admin/content/categories?content_type=${encodeURIComponent(contentType)}`).then((data) => { cmsTaxonomy=data.items || []; renderTaxonomy(fieldset,cmsTaxonomy,selected); });
  const recipeMove = (items, index, offset) => {
    if (!contentStudioMove(items, index, offset, () => setContentEditorDirty(true))) return;
    renderRecipeEditor();
  };
  const recipeRowButton = (label, handler) => {
    const button = text("button", label, "secondary");
    button.type = "button";
    button.addEventListener("click", handler);
    return button;
  };
  const renderRecipeEditor = () => {
    recipeIngredientsList.replaceChildren();
    recipeIngredients.forEach((item, index) => {
      const row = document.createElement("div"); row.className = "recipe-editor-row";
      const name = document.createElement("input"); name.type = "text"; name.maxLength = 200; name.placeholder = "Ингредиент"; name.value = item.name;
      const amount = document.createElement("input"); amount.type = "text"; amount.maxLength = 100; amount.placeholder = "Количество"; amount.value = item.amount || "";
      name.addEventListener("input", () => { item.name = name.value; setContentEditorDirty(true); });
      amount.addEventListener("input", () => { item.amount = amount.value; setContentEditorDirty(true); });
      const controls = document.createElement("div"); controls.className = "recipe-editor-actions";
      controls.append(recipeRowButton("↑", () => recipeMove(recipeIngredients, index, -1)), recipeRowButton("↓", () => recipeMove(recipeIngredients, index, 1)), recipeRowButton("Удалить", () => { recipeIngredients.splice(index, 1); setContentEditorDirty(true); renderRecipeEditor(); }));
      row.append(name, amount, controls); recipeIngredientsList.append(row);
    });
    recipeStepsList.replaceChildren();
    recipeSteps.forEach((item, index) => {
      const row = document.createElement("div"); row.className = "recipe-editor-row";
      const instruction = document.createElement("textarea"); instruction.maxLength = 2000; instruction.placeholder = `Шаг ${index + 1}`; instruction.value = item.instruction;
      instruction.addEventListener("input", () => { item.instruction = instruction.value; setContentEditorDirty(true); });
      const controls = document.createElement("div"); controls.className = "recipe-editor-actions";
      controls.append(recipeRowButton("↑", () => recipeMove(recipeSteps, index, -1)), recipeRowButton("↓", () => recipeMove(recipeSteps, index, 1)), recipeRowButton("Удалить", () => { recipeSteps.splice(index, 1); setContentEditorDirty(true); renderRecipeEditor(); }));
      row.append(instruction, controls); recipeStepsList.append(row);
    });
  };
  const observeStudioCover = (visual, item, cover) => {
    if (!cover || !("IntersectionObserver" in window)) return;
    if (!studioCoverObserver) {
      const generation = studioCoverGeneration;
      studioCoverObserver = new IntersectionObserver((entries, observer) => {
        entries.filter((entry) => entry.isIntersecting).forEach((entry) => {
          observer.unobserve(entry.target);
          const load = entry.target._studioCoverLoad;
          if (load) load(generation);
        });
      }, {rootMargin: "180px 0px"});
    }
    visual._studioCoverLoad = (generation) => fetch(`/api/admin/content/cms/${encodeURIComponent(item.content_id)}/media/${encodeURIComponent(cover.media_id)}`, {
      headers: {Authorization: `Bearer ${sessionToken}`}, cache: "no-store", credentials: "omit",
    }).then((response) => {
      if (!response.ok) throw new Error("content_cover_unavailable");
      return response.blob();
    }).then((blob) => {
      if (generation !== studioCoverGeneration || !visual.isConnected) return;
      const url=URL.createObjectURL(blob); studioCoverUrls.push(url);
      const image=document.createElement("img"); image.src=url; image.alt=`Обложка ${item.title}`;
      visual.replaceChildren(image);
    }).catch(() => {
      if (generation === studioCoverGeneration && visual.isConnected) visual.replaceChildren(text("span", "Обложка недоступна"));
    });
    studioCoverObserver.observe(visual);
  };
  const openCmsContentPreview = (contentId) => loadCmsContentDetails(contentId).then(() => {
    contentStudioWorkspace.classList.add("preview-active");
    contentStudioTabs.querySelectorAll("[data-studio-panel]").forEach((button) => button.classList.toggle("active", button.dataset.studioPanel === "preview"));
    contentLivePreview.scrollIntoView({behavior: "smooth", block: "start"});
  });
  const cmsContentCard = (item) => {
    const article = document.createElement("article");
    article.className = "card studio-content-card";
    const button = document.createElement("button"); button.type = "button"; button.className = "studio-card-open";
    const typeLabel = cmsTypeLabel(item.content_type);
    const cover=(item.media || []).find((entry)=>entry.media_type==="cover");
    const visual=document.createElement("div"); visual.className="studio-card-cover";
    visual.append(text("span",cover ? "Обложка" : typeLabel));
    observeStudioCover(visual, item, cover);
    button.append(visual,text("p", typeLabel, "eyebrow"), text("h2", item.title));
    const badges = document.createElement("div");
    badges.className = "badges";
    badges.append(text("span", cmsStatusLabel(item.status), `badge studio-status ${item.status}`));
    if (item.access_level === "free") badges.append(text("span", "Бесплатно", "badge member-free-badge"));
    const categories=(item.categories || []).map((entry)=>entry.title).join(", ") || item.category || "Без категории";
    button.append(badges,text("p",`${item.duration_seconds ? formatDuration(item.duration_seconds)+" · " : ""}${categories}`));
    if(!item.media_ready && ["lesson","meditation"].includes(item.content_type)) button.append(text("p",item.content_type==="lesson" ? "Не добавлено видео" : "Не добавлено аудио или видео","studio-warning"));
    button.append(text("small",`Обновлено ${formatDate(item.updated_at)}`));
    button.addEventListener("click", () => loadCmsContentDetails(item.content_id));
    const actions=document.createElement("details"); actions.className="studio-card-actions studio-card-menu";
    const summary=text("summary","⋯"); summary.setAttribute("aria-label",`Действия: ${item.title}`); actions.append(summary);
    const menu=document.createElement("div"); menu.className="studio-card-menu-items";
    const addAction=(label, handler) => { const action=text("button",label,"secondary"); action.type="button"; action.addEventListener("click",handler); menu.append(action); };
    if(item.status==="draft") {
      addAction("Редактировать",()=>loadCmsContentDetails(item.content_id));
      addAction("Предпросмотр",()=>openCmsContentPreview(item.content_id));
      addAction("Опубликовать",()=>loadCmsContentDetails(item.content_id).then(previewContentLifecycle).catch(showApiError));
      addAction("Удалить черновик",()=>loadCmsContentDetails(item.content_id).then(() => { contentLifecycleMode="delete"; return previewContentLifecycle(); }).catch(showApiError));
    } else if(item.status==="published") {
      addAction("Редактировать новую версию",()=>loadCmsContentDetails(item.content_id).then(createContentRevision).catch(showApiError));
      addAction("Посмотреть в клубе",()=>loadMemberLesson(item.content_id,item.content_type).catch(showApiError));
      addAction("Архивировать",()=>loadCmsContentDetails(item.content_id).then(previewContentLifecycle).catch(showApiError));
    } else {
      addAction("Открыть",()=>loadCmsContentDetails(item.content_id));
    }
    actions.append(menu);
    article.append(button,actions);
    return article;
  };
  const renderVersionDetails = (version) => {
    const domain = version.content_type === "recipe"
      ? `${version.ingredients.length} ингредиентов · ${version.steps.length} шагов`
      : version.content_type === "nutrition_material" ? version.nutrition_body : null;
    contentVersionHistory.prepend(detailCard(`Версия ${version.version}`, [
      ["Событие", version.event_type], ["Статус", version.status],
      ["Название", version.title], ["Описание", version.description],
      ["Длительность", version.duration_seconds ? formatDuration(version.duration_seconds) : null],
      ["Медиа", version.media.map((entry) => `${entry.media_type} v${entry.version}`).join(", ") || "Нет"],
      ["Данные", domain], ["Администратор", `ID ${version.admin_id}`],
      ["Дата", formatDate(version.created_at)],
    ]));
  };
  const loadContentVersions = (contentId) => api(`/api/admin/content/cms/${encodeURIComponent(contentId)}/versions`).then((data) => {
    contentVersionHistory.replaceChildren();
    contentVersionHistoryEmpty.hidden = data.items.length !== 0;
    data.items.forEach((version) => {
      const button = text("button", `v${version.version} · ${version.event_type} · ${formatDate(version.created_at)}`, "secondary");
      button.type = "button";
      button.addEventListener("click", () => api(`/api/admin/content/cms/${encodeURIComponent(contentId)}/versions/${encodeURIComponent(version.version_id)}`).then(renderVersionDetails).catch(showApiError));
      contentVersionHistory.append(button);
    });
  });
  const resetContentLifecycle = (item) => {
    contentLifecycleActionId = null;
    contentLifecycleMode = item.status === "published" ? "archive" : "publish";
    contentLifecycleCard.hidden = true;
    contentLifecycleTitle.textContent = item.status === "published" ? "Архивирование" : "Публикация";
    contentLifecyclePreviewButton.textContent = item.status === "published" ? "Предпросмотр архивирования" : "Предпросмотр публикации";
    contentLifecycleMessage.textContent = item.status === "published"
      ? "Архивирование требует отдельного подтверждения. История и данные сохранятся."
      : "Публикация требует финального server-side preview и подтверждения.";
    contentLifecyclePreview.hidden = true;
    contentLifecycleConfirm.hidden = true;
    contentLifecycleCancel.hidden = true;
    contentLifecyclePreviewButton.hidden = item.status === "archived";
  };
  const previewContentLifecycle = () => {
    if (!currentCmsContent || !contentLifecycleMode) return Promise.resolve();
    const mode = contentLifecycleMode;
    contentLifecycleCard.hidden = false;
    if (mode === "delete") contentLifecycleTitle.textContent = "Удалить черновик?";
    contentLifecycleMessage.textContent = "Проверяем финальное состояние…";
    return writeAdminJson("POST", `/api/admin/content/cms/${encodeURIComponent(currentCmsContent.content_id)}/${mode}-preview`, {expected_version: currentCmsContent.version}).then((preview) => {
      contentLifecycleActionId = preview.action_id;
      const mediaTypes = new Set((preview.media || []).map((entry) => entry.media_type));
      const checklist = document.createElement("ul"); checklist.className = "studio-publish-checklist";
      if (mode === "publish") {
        checklist.append(text("li", "Обязательно", "checklist-heading"));
        const required = [[Boolean(preview.title), "Название"]];
        if (preview.content_type === "lesson") required.push([Boolean(preview.duration_seconds), "Длительность"], [mediaTypes.has("video"), "Видео"]);
        if (preview.content_type === "meditation") required.push([Boolean(preview.duration_seconds), "Длительность"], [mediaTypes.has("audio") || mediaTypes.has("video"), "Аудио или видео"]);
        if (preview.content_type === "recipe") required.push([Boolean(preview.domain.ingredients.length), "Ингредиенты"], [Boolean(preview.domain.steps.length), "Шаги"]);
        if (preview.content_type === "nutrition_material") required.push([Boolean((preview.domain.body || "").trim()), "Основной текст"]);
        required.forEach(([ready, label]) => checklist.append(text("li", `${ready ? "✓" : "—"} ${label}`, ready ? "ready" : "missing")));
        checklist.append(text("li", "Рекомендуется", "checklist-heading"));
        [[Boolean((preview.categories || []).length || preview.category), "Категория"], [mediaTypes.has("cover"), "Обложка"]]
          .forEach(([ready, label]) => checklist.append(text("li", `${ready ? "✓" : "—"} ${label}`, ready ? "ready" : "recommended")));
      }
      contentLifecyclePreview.replaceChildren(
        text("strong", preview.title),
        text("span", mode === "delete" ? "Черновик" : cmsTypeLabel(preview.content_type)),
        text("span", mode === "delete" ? `Связанных медиа: ${preview.media_count}` : preview.description || "Без описания"),
        text("span", mode === "delete" ? "История и связанные данные сохранятся для аудита." : `Медиа: ${(preview.media || []).map((entry) => entry.media_type).join(", ") || "нет"}`),
        checklist,
        text("span", `Действительно до ${formatDate(preview.preview_expires_at)}`)
      );
      contentLifecyclePreview.hidden = false;
      contentLifecycleConfirm.textContent = mode === "archive" ? "Архивировать" : mode === "delete" ? "Удалить" : "Опубликовать";
      contentLifecycleConfirm.hidden = false; contentLifecycleCancel.hidden = false;
      contentLifecycleMessage.textContent = mode === "delete"
        ? "Материал исчезнет из списка черновиков. Опубликованный контент эта операция не затрагивает."
        : mode === "archive"
        ? "Материал перестанет отображаться в будущей пользовательской библиотеке, но история и данные сохранятся."
        : "Проверьте итоговые данные перед публикацией.";
      contentLifecycleCard.scrollIntoView({behavior: "smooth", block: "center"});
    }).catch((error) => { contentLifecycleMessage.textContent = contentErrorMessage(error); });
  };
  const confirmContentLifecycle = () => {
    if (!currentCmsContent || !contentLifecycleActionId || !contentLifecycleMode) return Promise.resolve();
    contentLifecycleConfirm.disabled = true;
    const mode = contentLifecycleMode;
    return writeAdminJson("POST", `/api/admin/content/cms/${encodeURIComponent(currentCmsContent.content_id)}/${mode}-confirm`, {action_id: contentLifecycleActionId})
      .then(() => mode === "delete" ? loadContent().then(() => { status.textContent="Черновик удалён"; }) : loadCmsContentDetails(currentCmsContent.content_id))
      .catch((error) => {
        contentLifecycleMessage.textContent = contentErrorMessage(error);
      }).finally(() => { contentLifecycleConfirm.disabled = false; });
  };
  const cancelContentLifecycle = () => {
    if (!currentCmsContent || !contentLifecycleActionId || !contentLifecycleMode) return Promise.resolve();
    return writeAdminJson("POST", `/api/admin/content/cms/${encodeURIComponent(currentCmsContent.content_id)}/${contentLifecycleMode}-cancel`, {action_id: contentLifecycleActionId})
      .then(() => { resetContentLifecycle(currentCmsContent); contentLifecycleCard.hidden = true; status.textContent = "Операция отменена"; })
      .catch(showApiError);
  };
  const loadCmsContentDetails = (contentId) => {
    clearContentMediaUrls();
    resetContentMediaDraft();
    status.textContent = "Загружаем черновик…";
    return api(`/api/admin/content/cms/${encodeURIComponent(contentId)}`).then((item) => {
      currentCmsContent = item;
      renderEditorLibrary();
      contentEditorTitle.textContent = item.title;
      contentEditorStatus.textContent = cmsStatusLabel(item.status);
      contentEditorStatus.className = `badge studio-status ${item.status}`;
      contentEditorMenu.hidden = true;
      contentEditorMore.setAttribute("aria-expanded", "false");
      contentEditorActions.hidden = item.status === "archived";
      contentBottomSave.hidden = item.status !== "draft";
      const metadata = detailCard("Состояние материала", [
        ["Статус", cmsStatusLabel(item.status)],
        ["Последнее изменение", formatDate(item.updated_at)],
        ["Готовность", item.media_ready ? "Основное медиа добавлено" : "Требуется проверить медиа"],
        ["Доступ", item.access_level === "free" ? "Бесплатно" : "По подписке"],
      ]);
      const description = document.createElement("article"); description.className = "card authoring-readable";
      description.append(text("h2", "Описание"));
      if (item.description) appendRestrictedText(description, item.description);
      else description.append(text("p", "Описание не заполнено.", "hint"));
      contentDetailsContent.replaceChildren(metadata, description);
      contentEditCard.hidden = item.status !== "draft";
      contentCreateRevision.hidden = item.status !== "published";
      contentMediaCard.hidden = item.status !== "draft";
      contentRecipeCard.hidden = item.status !== "draft" || item.content_type !== "recipe";
      contentNutritionCard.hidden = item.status !== "draft" || item.content_type !== "nutrition_material";
      contentVideoControl.hidden = item.content_type === "recipe" || item.content_type === "nutrition_material";
      contentAudioControl.hidden = item.content_type !== "meditation";
      contentEditTitle.value = item.title;
      contentEditCategory.value = item.category || "";
      loadTaxonomy(item.content_type, contentEditTaxonomy, (item.categories || []).map((entry) => entry.id)).catch(showApiError);
      contentEditDescription.value = item.description || "";
      contentEditDuration.value = item.duration_seconds ? formatDuration(item.duration_seconds) : "";
      contentEditDuration.closest("label").hidden = item.content_type === "nutrition_material";
      document.getElementById("content-edit-save").hidden = item.content_type === "nutrition_material";
      contentEditOrder.value = item.sort_order;
      contentEditAccess.value = item.access_level || "paid";
      contentEditMessage.textContent = "Изменения сохраняются только по кнопке.";
      recipeIngredients = item.recipe ? item.recipe.ingredients.map((entry) => ({name: entry.name, amount: entry.amount || ""})) : [];
      recipeSteps = item.recipe ? item.recipe.steps.map((entry) => ({instruction: entry.instruction})) : [];
      contentNutritionBody.value = item.nutrition ? item.nutrition.body : "";
      renderRecipeEditor();
      renderContentMedia(item);
      resetContentLifecycle(item);
      setContentEditorDirty(false);
      const previewOnly = item.status !== "draft";
      contentStudioWorkspace.classList.toggle("preview-active", previewOnly);
      contentStudioTabs.querySelectorAll("[data-studio-panel]").forEach((button) => button.classList.toggle("active", button.dataset.studioPanel === (previewOnly ? "preview" : "editor")));
      renderContentLivePreview(item);
      contentEditorState.textContent = item.status === "draft" ? "Все изменения сохранены ✓" : cmsStatusLabel(item.status);
      contentBottomPublish.textContent = item.status === "published" ? "Архивировать" : "Опубликовать";
      contentBottomPublish.disabled = item.status === "archived" || contentEditorDirty;
      loadContentVersions(item.content_id).catch(showApiError);
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
  const fetchContentCover = (item, media) => {
    const generation = contentMediaGeneration;
    return fetch(
    `/api/admin/content/cms/${encodeURIComponent(item.content_id)}/media/${encodeURIComponent(media.media_id)}`,
    {headers: {Authorization: `Bearer ${sessionToken}`}, cache: "no-store", credentials: "omit"}
    ).then((response) => {
    if (!response.ok) throw new Error("content_media_preview_failed");
    return response.blob();
  }).then((blob) => {
    if (generation !== contentMediaGeneration || !currentCmsContent || currentCmsContent.content_id !== item.content_id) return;
    if (contentMediaAttachedCoverUrl) URL.revokeObjectURL(contentMediaAttachedCoverUrl);
    contentMediaAttachedCoverUrl = URL.createObjectURL(blob);
    contentCoverCurrent.replaceChildren(mediaImage(contentMediaAttachedCoverUrl, `Обложка ${item.title}`));
    if (item.status === "draft") {
      const remove=text("button","Удалить","secondary content-media-remove"); remove.type="button";
      remove.addEventListener("click",()=>previewContentMediaRemove(media,"обложку")); contentCoverCurrent.append(remove);
    }
    renderContentLivePreview(item);
  }).catch(() => {
    if (generation === contentMediaGeneration && currentCmsContent && currentCmsContent.content_id === item.content_id) {
      contentCoverCurrent.replaceChildren(text("span", "Обложка недоступна", "schedule-image-loading"));
    }
  });
  };
  function renderContentMedia(item) {
    const cover = (item.media || []).find((entry) => entry.media_type === "cover");
    const video = (item.media || []).find((entry) => entry.media_type === "video");
    const audio = (item.media || []).find((entry) => entry.media_type === "audio");
    contentCoverCurrent.replaceChildren(text("span", cover ? "Загружаем обложку…" : "Обложка не прикреплена", "schedule-image-loading"));
    contentVideoCurrent.textContent = video
      ? `MP4 · ${Math.ceil(video.size_bytes / 1024 / 1024)} МиБ · версия ${video.version}`
      : "Видео не прикреплено";
    contentAudioCurrent.textContent = audio
      ? `MP3 · ${Math.ceil(audio.size_bytes / 1024 / 1024)} МиБ · версия ${audio.version}`
      : "Аудио не прикреплено";
    if (cover) fetchContentCover(item, cover);
    const addRemove = (host, media, label) => {
      if (!media || item.status !== "draft") return;
      const remove = text("button", "Удалить", "secondary content-media-remove"); remove.type="button";
      remove.addEventListener("click", () => previewContentMediaRemove(media, label)); host.append(remove);
    };
    addRemove(contentVideoCurrent, video, "видео");
    addRemove(contentAudioCurrent, audio, "аудио");
  }
  const previewContentMediaRemove = (media, label) => {
    if (!currentCmsContent || currentCmsContent.status !== "draft") return Promise.resolve();
    const mode = `media/${encodeURIComponent(media.media_id)}/remove`;
    contentLifecycleMode = mode;
    contentLifecycleTitle.textContent = `Удалить ${label}?`;
    contentLifecycleCard.hidden = false;
    contentLifecycleMessage.textContent = "Связь с материалом будет отключена. Telegram-файл физически не удаляется.";
    return writeAdminJson("POST", `/api/admin/content/cms/${encodeURIComponent(currentCmsContent.content_id)}/${mode}-preview`, {expected_version:currentCmsContent.version}).then((preview) => {
      contentLifecycleActionId=preview.action_id;
      contentLifecyclePreview.replaceChildren(text("strong",currentCmsContent.title),text("span",`Медиа: ${media.media_type}`),text("span","История публикаций не изменится."));
      contentLifecyclePreview.hidden=false; contentLifecycleConfirm.textContent="Удалить";
      contentLifecycleConfirm.hidden=false; contentLifecycleCancel.hidden=false; contentLifecyclePreviewButton.hidden=true;
    }).catch((error)=>{ contentLifecycleMessage.textContent=contentErrorMessage(error); });
  };
  const showLocalContentMedia = (mediaType) => {
    const file = mediaType === "cover" ? contentCoverFile.files[0] : mediaType === "audio" ? contentAudioFile.files[0] : contentVideoFile.files[0];
    if (contentMediaLocalUrl) URL.revokeObjectURL(contentMediaLocalUrl);
    contentMediaLocalUrl = file ? URL.createObjectURL(file) : null;
    contentMediaLocalType = file ? mediaType : null;
    contentMediaPreview.replaceChildren();
    if (!file) contentMediaPreview.append(text("span", "Выберите файл", "schedule-image-loading"));
    else if (mediaType === "cover") { contentMediaPreview.append(mediaImage(contentMediaLocalUrl, "Локальный просмотр обложки")); renderContentLivePreview(); }
    else contentMediaPreview.append(text("span", `Локально выбрано ${mediaType === "audio" ? "аудио" : "видео"}: ${file.name}`, "schedule-image-loading"));
  };
  const validateContentMedia = (mediaType) => {
    if (!currentCmsContent || currentCmsContent.status !== "draft") return Promise.resolve();
    if (!contentStudioCanStartMedia(contentEditorDirty)) {
      contentMediaMessage.textContent = "Сначала сохраните изменения материала, затем загрузите медиа.";
      contentMediaConfirmation.hidden = false;
      return Promise.resolve();
    }
    const file = mediaType === "cover" ? contentCoverFile.files[0] : mediaType === "audio" ? contentAudioFile.files[0] : contentVideoFile.files[0];
    if (!file) {
      contentMediaMessage.textContent = "Выберите файл.";
      contentMediaConfirmation.hidden = false;
      return Promise.resolve();
    }
    if (mediaType === "video" && file.size > 20 * 1024 * 1024) {
      contentMediaMessage.textContent = `Видео слишком большое: ${(file.size / 1024 / 1024).toFixed(1)} МБ. Текущий максимум — 20 МБ.`;
      contentMediaConfirmation.hidden = false;
      return Promise.resolve();
    }
    if (mediaType === "video" && file.type !== "video/mp4" && !file.name.toLowerCase().endsWith(".mp4")) {
      contentMediaMessage.textContent = "Этот формат пока не поддерживается. Загрузите MP4.";
      contentMediaConfirmation.hidden = false;
      return Promise.resolve();
    }
    if (mediaType === "audio" && !file.name.toLowerCase().endsWith(".mp3")) {
      contentMediaMessage.textContent = "Этот формат пока не поддерживается. Загрузите MP3.";
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
          text("span", mediaType === "cover" ? "Обложка" : mediaType === "audio" ? "Аудио" : "Видео"),
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
          : mediaType === "audio" ? "Этот формат пока не поддерживается. Загрузите MP3."
          : "Файл не прошёл безопасную проверку.";
      });
  };
  const confirmContentMedia = () => {
    if (!contentMediaUploadId || !currentCmsContent) return Promise.resolve();
    if (!contentStudioCanStartMedia(contentEditorDirty)) {
      contentMediaMessage.textContent = "Сначала сохраните изменения материала, затем загрузите медиа.";
      return Promise.resolve();
    }
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
  const createRecipePayload = () => ({
    ingredients: contentCreateIngredients.value.split("\n").map((line) => line.trim()).filter(Boolean).map((line, index) => {
      const [name, ...amount] = line.split("|");
      return {name: name.trim(), amount: amount.join("|").trim() || null, sort_order: index};
    }),
    steps: contentCreateSteps.value.split("\n").map((line) => line.trim()).filter(Boolean).map((instruction, index) => ({step_number: index + 1, instruction})),
  });
  const attachAuthoringMedia = (contentId, mediaType, file) => {
    if (!file) return Promise.resolve();
    if (mediaType === "video" && file.size > 20 * 1024 * 1024) return Promise.reject(new Error("content_video_too_large"));
    if (mediaType === "video" && file.type !== "video/mp4" && !file.name.toLowerCase().endsWith(".mp4")) return Promise.reject(new Error("unsupported_video_format"));
    if (mediaType === "audio" && !file.name.toLowerCase().endsWith(".mp3")) {
      return Promise.reject(new Error("unsupported_audio_format"));
    }
    const form = new FormData(); form.append("media_type", mediaType); form.append("file", file);
    return postAdmin(`/api/admin/content/cms/${encodeURIComponent(contentId)}/media-preview`, form)
      .then((upload) => postAdmin(`/api/admin/content/media/uploads/${encodeURIComponent(upload.upload_id)}/confirm`))
      .then((result) => { if (result.status !== "completed") throw new Error(result.failure_category || result.status); });
  };
  const createCmsDraft = () => {
    const files = [
      ["cover", contentCreateCoverFile.files[0]],
      ["video", contentCreateVideoFile.files[0]],
      ["audio", contentCreateAudioFile.files[0]],
    ];
    contentCreateMessage.textContent = "Создаём черновик…";
    const payload = {
      content_type: contentCreateType.value, title: contentCreateTitle.value,
      category: contentCreateCategory.value || null,
      description: contentCreateDescription.value || null,
      duration_seconds: contentCreateType.value === "nutrition_material" ? null : parseDuration(contentCreateDuration.value),
      access_level: contentCreateAccess.value,
    };
    if (contentCreateType.value !== "nutrition_material") payload.category_ids = selectedTaxonomyIds(contentCreateTaxonomy);
    if (contentCreateType.value === "nutrition_material") payload.body = contentCreateBody.value;
    const recipePayload = contentCreateType.value === "recipe" ? createRecipePayload() : null;
    return contentStudioCreateDraft({
      files,
      createDraft: () => writeAdminJson("POST", "/api/admin/content/drafts", payload),
      saveDomain: (item) => recipePayload
        ? writeAdminJson("PUT", `/api/admin/content/cms/${encodeURIComponent(item.content_id)}/recipe`, {expected_version: item.version, ...recipePayload})
        : Promise.resolve(item),
      attachMedia: (item, mediaType, file) => attachAuthoringMedia(item.content_id, mediaType, file),
      openDraft: (item) => loadCmsContentDetails(item.content_id),
    }).then((result) => {
      if (result.status === "preflight_failed") {
        contentCreateMessage.textContent = result.error;
        return null;
      }
      if (result.status === "media_failed") {
        contentMediaMessage.textContent = "Черновик сохранён. Медиа загрузить не удалось. Добавьте файл ещё раз.";
        status.textContent = "Черновик сохранён, медиа требует повторной загрузки";
        return null;
      }
      if (result.status === "domain_failed") {
        contentEditMessage.textContent = "Черновик сохранён, но дополнительные данные сохранить не удалось. Проверьте материал и повторите сохранение.";
        status.textContent = "Черновик сохранён, дополнительные данные требуют проверки";
        return null;
      }
      const item = result.draft;
      contentCreateTitle.value = ""; contentCreateCategory.value = "";
      contentCreateAccess.value = "paid";
      contentCreateDescription.value = ""; contentCreateDuration.value = "";
      contentCreateBody.value = ""; contentCreateIngredients.value = ""; contentCreateSteps.value = "";
      contentCreateCoverFile.value = ""; contentCreateVideoFile.value = ""; contentCreateAudioFile.value = "";
      return loadCmsContentDetails(item.content_id).then(() => item);
    }).catch((error) => {
      contentCreateMessage.textContent = error.message === "unsupported_audio_format"
        ? "Этот формат пока не поддерживается. Загрузите MP3."
        : error.message === "content_video_too_large" ? "Видео слишком большое. Текущий максимум — 20 МБ."
        : error.message === "unsupported_video_format" ? "Этот формат пока не поддерживается. Загрузите MP4."
        : contentErrorMessage(error, {content_type: contentCreateType.value});
      return null;
    });
  };
  const saveCmsDraft = ({reload = true} = {}) => {
    if (!currentCmsContent) return Promise.resolve();
    contentEditMessage.textContent = "Сохраняем…";
    return writeAdminJson(
      "PATCH", `/api/admin/content/cms/${encodeURIComponent(currentCmsContent.content_id)}`,
      {expected_version: currentCmsContent.version, title: contentEditTitle.value,
       category: contentEditCategory.value || null,
       category_ids: selectedTaxonomyIds(contentEditTaxonomy),
       description: contentEditDescription.value || null,
       duration_seconds: parseDuration(contentEditDuration.value),
       sort_order: Number(contentEditOrder.value), access_level: contentEditAccess.value}
    ).then((item) => {
      currentCmsContent = {...currentCmsContent, ...item};
      if (!reload) return item;
      return loadCmsContentDetails(item.content_id).then(() => { setContentEditorDirty(false); return item; });
    }).catch((error) => {
      contentEditMessage.textContent = contentErrorMessage(error);
      throw error;
    });
  };
  const createContentRevision = () => {
    if (!currentCmsContent || currentCmsContent.status !== "published") return Promise.resolve();
    contentCreateRevision.disabled = true;
    return writeAdminJson("POST", `/api/admin/content/cms/${encodeURIComponent(currentCmsContent.content_id)}/revision`, {})
      .then((item) => loadCmsContentDetails(item.content_id))
      .catch((error) => { status.textContent = contentErrorMessage(error); })
      .finally(() => { contentCreateRevision.disabled = false; });
  };
  const saveAndPreviewPublish = () => {
    if (!currentCmsContent || currentCmsContent.status !== "draft") return Promise.resolve();
    return saveCurrentContent().then(() => previewContentLifecycle()).catch(() => null);
  };
  const saveRecipe = ({reload = true} = {}) => {
    if (!currentCmsContent || currentCmsContent.content_type !== "recipe") return Promise.resolve();
    recipeEditMessage.textContent = "Сохраняем…";
    return writeAdminJson("PUT", `/api/admin/content/cms/${encodeURIComponent(currentCmsContent.content_id)}/recipe`, {
      expected_version: currentCmsContent.version,
      ingredients: recipeIngredients.map((item, index) => ({name: item.name, amount: item.amount || null, sort_order: index})),
      steps: recipeSteps.map((item, index) => ({step_number: index + 1, instruction: item.instruction})),
    }).then((item) => {
      currentCmsContent = {...currentCmsContent, ...item};
      if (!reload) return item;
      return loadCmsContentDetails(currentCmsContent.content_id).then(() => { setContentEditorDirty(false); return item; });
    }).catch((error) => {
      recipeEditMessage.textContent = contentErrorMessage(error);
      throw error;
    });
  };
  const saveNutrition = () => {
    if (!currentCmsContent || currentCmsContent.content_type !== "nutrition_material") return Promise.resolve();
    contentNutritionMessage.textContent = "Сохраняем…";
    return writeAdminJson("PUT", `/api/admin/content/cms/${encodeURIComponent(currentCmsContent.content_id)}/nutrition`, {
      expected_version: currentCmsContent.version,
      title: contentEditTitle.value,
      category: contentEditCategory.value || null,
      description: contentEditDescription.value || null,
      duration_seconds: null,
      sort_order: Number(contentEditOrder.value),
      access_level: contentEditAccess.value,
      body: contentNutritionBody.value,
    }).then(() => loadCmsContentDetails(currentCmsContent.content_id).then(() => setContentEditorDirty(false))).catch((error) => {
      contentNutritionMessage.textContent = contentErrorMessage(error);
      throw error;
    });
  };
  const saveCurrentContent = () => {
    if (!currentCmsContent || currentCmsContent.status !== "draft") return Promise.resolve(currentCmsContent);
    if (currentCmsContent.content_type === "nutrition_material") return saveNutrition();
    if (currentCmsContent.content_type === "recipe") {
      return contentStudioSaveRecipe({
        saveMetadata: () => saveCmsDraft({reload: false}),
        saveRecipe: () => saveRecipe({reload: false}),
        reload: () => loadCmsContentDetails(currentCmsContent.content_id),
      })
        .then(() => { setContentEditorDirty(false); return currentCmsContent; });
    }
    return saveCmsDraft();
  };

  hydrateMemberIcons();
  const installMemberAdminCreateActions = () => {
    const targets = [
      ["#member-home .member-section-title", "lesson", "+ Добавить"],
      ["#member-library .member-page-heading", "lesson", "+ Добавить тренировку"],
      ["#member-meditations .member-page-heading", "meditation", "+ Добавить медитацию"],
      ["#member-recipes .member-page-heading", "recipe", "+ Добавить рецепт"],
      ["#member-nutrition .member-page-heading", "nutrition_material", "+ Добавить материал"],
    ];
    targets.forEach(([selector, type, label]) => {
      const target = document.querySelector(selector);
      if (!target || target.querySelector("[data-create-content]")) return;
      const button = text("button", label, "member-admin-create member-button");
      button.type = "button"; button.dataset.createContent = type;
      button.addEventListener("click", () => {
        contentCreateType.value = type;
        showScreen("content-create");
        memberShellHeader.hidden = false;
        memberBottomNav.hidden = false;
        loadTaxonomy(type, contentCreateTaxonomy).catch(showApiError);
      });
      target.append(button);
    });
  };
  const closeAdminHeaderPanels = (except = null) => {
    [[adminSearchResults, null], [adminNotificationPanel, adminNotifications], [adminProfilePanel, adminProfileToggle]].forEach(([panel, toggle]) => {
      if (!panel || panel === except) return;
      panel.hidden = true;
      if (toggle) toggle.setAttribute("aria-expanded", "false");
    });
  };
  const adminResultGroup = (label, items) => {
    const group = document.createElement("div"); group.className = "admin-result-group";
    group.append(text("h3", label));
    items.forEach((item) => {
      const button = document.createElement("button"); button.type = "button"; button.className = "admin-result-item";
      const copy = document.createElement("span"); copy.append(text("strong", item.title), text("small", item.meta));
      button.append(copy, text("i", "→"));
      button.addEventListener("click", () => { closeAdminHeaderPanels(); adminGlobalSearch.value = ""; item.open(); });
      group.append(button);
    });
    return group;
  };
  const runAdminGlobalSearch = () => {
    const query = adminGlobalSearch.value.trim();
    const generation = ++adminSearchGeneration;
    if(adminSearchController) adminSearchController.abort();
    adminSearchController=new AbortController();
    const signal=adminSearchController.signal;
    adminSearchResults.hidden = false;
    closeAdminHeaderPanels(adminSearchResults);
    if (query.length < 2) {
      adminSearchResults.replaceChildren(text("p", "Введите минимум 2 символа.", "admin-panel-state"));
      return Promise.resolve();
    }
    adminSearchResults.replaceChildren(text("p", "Ищем…", "admin-panel-state"));
    const userParams = new URLSearchParams({limit:"6",status:"all",q:query});
    return Promise.all([
      api(`/api/admin/users?${userParams.toString()}`,{signal}),
      api("/api/admin/content/cms?status=all&limit=50",{signal}),
      api("/api/admin/failed-subscriptions?state=attention&limit=12",{signal}),
    ]).then(([users,content,failed]) => {
      if (generation !== adminSearchGeneration) return;
      const contentLabels = {lesson:"Тренировка",meditation:"Медитация",recipe:"Рецепт",nutrition_material:"Материал"};
      const contentMatches = content.items.filter((item) => adminSearchMatches([item.title,item.content_type,contentLabels[item.content_type],...(item.categories || []).map((entry)=>entry.title)],query)).slice(0,6);
      const taskMatches = failed.items.filter((item) => adminSearchMatches([item.username,item.first_name,item.reason_label,item.status],query)).slice(0,6);
      const groups = [];
      if (users.items.length) groups.push(adminResultGroup("Участники", users.items.map((user)=>({title:user.first_name || (user.username ? `@${user.username}` : "Участник"),meta:`${user.username ? `@${user.username} · ` : ""}${statusLabels[user.access_status] || user.access_status}`,open:()=>loadUserDetails(user.telegram_id)}))));
      if (contentMatches.length) groups.push(adminResultGroup("Контент · 50 последних", contentMatches.map((item)=>({title:item.title,meta:`${contentLabels[item.content_type] || "Материал"} · ${item.status}`,open:()=>loadCmsContentDetails(item.content_id)}))));
      if (taskMatches.length) groups.push(adminResultGroup("Задачи", taskMatches.map((item)=>({title:item.username ? `@${item.username}` : (item.first_name || "Проблема продления"),meta:`${item.reason_label} · ${failedStatusLabels[item.status] || item.status}`,open:()=>loadFailedSubscriptionDetails(item.operation_id)}))));
      adminSearchResults.replaceChildren(...(groups.length ? groups : [text("p", "Ничего не найдено.", "admin-panel-state")]));
    }).catch((error) => {
      if(error.name === "AbortError") return;
      if (generation === adminSearchGeneration) adminSearchResults.replaceChildren(text("p", "Поиск временно недоступен.", "admin-panel-state"));
      if (error.message === "session_ended" || error.message === "access_revoked") showApiError(error);
    });
  };
  const adminNotificationReadKeys = () => adminNotificationReadState;
  const loadAdminNotificationAcknowledgements = () => api("/api/admin/notification-acknowledgements").then((data) => {
    hydrateAdminNotificationReadState(adminNotificationReadState,data.notification_keys);
    adminNotificationResolvedState.clear(); adminNotificationArchivedState.clear();
    (data.states || []).forEach((state)=>{
      if(state.resolved_at) adminNotificationResolvedState.add(state.notification_key);
      if(state.archived_at) adminNotificationArchivedState.add(state.notification_key);
    });
    adminNotificationAckError="";
  }).catch((error)=>{
    adminNotificationReadState.clear();
    adminNotificationResolvedState.clear();
    adminNotificationArchivedState.clear();
    adminNotificationAckError="Не удалось загрузить отметки. Уведомления считаются непрочитанными.";
    if (error.message === "session_ended" || error.message === "access_revoked") throw error;
  });
  const markAdminNotificationRead = (key) => {
    if (adminNotificationReadState.has(key) || adminNotificationAckPending.has(key)) return Promise.resolve();
    adminNotificationAckPending.add(key); adminNotificationAckError=""; renderAdminNotificationPanel();
    return persistAdminNotificationRead({key,readState:adminNotificationReadState,persist:(notificationKey)=>writeAdminJson("PUT","/api/admin/notification-acknowledgements",{notification_key:notificationKey})}).then(()=>{
      adminNotificationAckError="";
      updateAdminNotificationBadge();
    }).catch((error)=>{
      adminNotificationAckError="Не удалось сохранить отметку. Уведомление осталось непрочитанным.";
      if (error.message === "session_ended" || error.message === "access_revoked") showApiError(error);
    }).finally(()=>{ adminNotificationAckPending.delete(key); renderAdminNotificationPanel(); renderNotificationCenter(); });
  };
  const updateAdminNotificationLifecycle = (key,action) => {
    if(adminNotificationAckPending.has(key)) return Promise.resolve();
    adminNotificationAckPending.add(key);
    return writeAdminJson("PUT","/api/admin/notification-acknowledgements",{notification_key:key,action}).then((state)=>{
      if(state.read_at) adminNotificationReadState.add(key);
      if(state.resolved_at) adminNotificationResolvedState.add(key); else adminNotificationResolvedState.delete(key);
      if(state.archived_at) adminNotificationArchivedState.add(key); else adminNotificationArchivedState.delete(key);
      renderAdminNotificationPanel(); renderNotificationCenter(); updateAdminNotificationBadge();
    }).catch(showApiError).finally(()=>adminNotificationAckPending.delete(key));
  };
  const buildAdminNotificationItems = (failed,gifts,deliveries,system) => [
    ...failed.map((item)=>({key:`failed:${item.operation_id}`,category:"failed",timestamp:item.updated_at,title:item.username ? `Проблема продления · @${item.username}` : "Проблема продления",meta:`${item.reason_label} · ${failedStatusLabels[item.status] || item.status} · ${formatDate(item.updated_at)}`,open:()=>loadFailedSubscriptionDetails(item.operation_id)})),
    ...deliveries.map((item)=>({key:`delivery:${item.delivery_id}`,category:"delivery",timestamp:item.updated_at || item.next_attempt_at,title:item.delivery_label,meta:`${item.explanation || item.status} · ${formatDate(item.updated_at || item.next_attempt_at)}`,open:()=>loadDeliveryDetails(item.delivery_id)})),
    ...gifts.map((item)=>({key:`gift:${item.gift_id}`,category:"gift",timestamp:item.updated_at || item.created_at,title:"Подарок требует проверки",meta:`${item.public_reference} · ${item.status_label} · ${formatDate(item.updated_at || item.created_at)}`,open:()=>loadGiftDetails(item.gift_id)})),
    ...(Number(system.scheduler.failed_last_24h || 0) && system.scheduler.latest_failed_incident ? [{key:adminSystemIncidentKey("scheduler",system.scheduler.latest_failed_incident),category:"system",title:"Ошибки планировщика",meta:`За 24 часа: ${system.scheduler.failed_last_24h}`,open:()=>loadSystem()}] : []),
    ...(Number(system.removals.retryable || 0) && system.removals.latest_retryable_incident ? [{key:adminSystemIncidentKey("removals",system.removals.latest_retryable_incident),category:"system",title:"Повторные удаления",meta:`Ожидают обработки: ${system.removals.retryable}`,open:()=>loadSystem()}] : []),
  ];
  const updateAdminNotificationBadge = () => {
    const read=adminNotificationReadKeys();
    const unread=[...adminCurrentNotificationKeys].filter((key)=>!read.has(key) && !adminNotificationResolvedState.has(key) && !adminNotificationArchivedState.has(key)).length+adminNotificationUnknownUnreadCount;
    setAttentionCount(unread);
    adminNotifications.classList.toggle("has-unread",unread>0);
  };
  const renderAdminNotificationPanel = () => {
    const read=adminNotificationReadKeys();
    const heading=document.createElement("header"); heading.append(text("strong","Уведомления"),text("small","Просмотр не означает решение проблемы"));
    const items=document.createElement("div"); items.className="admin-notification-items";
    const page=adminNotificationPanelPage(adminNotificationItems.filter((item)=>!adminNotificationArchivedState.has(item.key)),adminNotificationDisplayLimit);
    page.visible.forEach((item)=>{
      const row=document.createElement("article"); row.className=`admin-notification-item${read.has(item.key) ? " read" : ""}`;
      const open=document.createElement("button"); open.type="button"; open.className="admin-notification-open"; open.append(text("strong",item.title),text("small",item.meta));
      open.addEventListener("click",()=>{ markAdminNotificationRead(item.key); closeAdminHeaderPanels(); item.open(); });
      const mark=document.createElement("button"); mark.type="button"; mark.className="admin-notification-mark"; mark.textContent=read.has(item.key) ? "Просмотрено" : (adminNotificationAckPending.has(item.key) ? "Сохраняем…" : "Прочитано"); mark.disabled=read.has(item.key) || adminNotificationAckPending.has(item.key); mark.addEventListener("click",()=>markAdminNotificationRead(item.key));
      row.append(open,mark); items.append(row);
    });
    if (page.remaining) {
      const more=document.createElement("button"); more.type="button"; more.className="admin-notification-more secondary"; more.textContent=`Показать ещё ${Math.min(MAX_NOTIFICATION_PANEL_ITEMS,page.remaining)}`;
      more.addEventListener("click",()=>{ adminNotificationDisplayLimit += MAX_NOTIFICATION_PANEL_ITEMS; renderAdminNotificationPanel(); });
      items.append(more);
    }
    if (adminNotificationPaginationIncomplete) items.append(text("p","Не все уведомления удалось загрузить.","admin-panel-state warning"));
    if (adminNotificationAckError) items.prepend(text("p",adminNotificationAckError,"admin-panel-state error"));
    if (!adminNotificationItems.length) items.append(text("p","Новых событий нет.","admin-panel-state"));
    adminNotificationPanel.replaceChildren(heading,items);
  };
  const notificationCenterFilteredItems = () => {
    const category=document.getElementById("notifications-category").value;
    const readFilter=document.getElementById("notifications-read-filter").value;
    const resolutionFilter=document.getElementById("notifications-resolution-filter").value;
    return adminNotificationItems.filter((item)=>{
      const isRead=adminNotificationReadState.has(item.key);
      const isResolved=adminNotificationResolvedState.has(item.key);
      return (category === "all" || item.category === category)
        && !adminNotificationArchivedState.has(item.key)
        && (readFilter === "all" || (readFilter === "read") === isRead)
        && (resolutionFilter === "all" || (resolutionFilter === "resolved") === isResolved);
    });
  };
  const renderNotificationCenter = () => {
    const host=document.getElementById("notifications-page-list");
    if (!host) return;
    const filtered=notificationCenterFilteredItems(); const page=adminNotificationPanelPage(filtered,adminNotificationPageLimit);
    const unresolved=[...adminCurrentNotificationKeys].filter((key)=>!adminNotificationResolvedState.has(key) && !adminNotificationArchivedState.has(key));
    const unread=[...adminCurrentNotificationKeys].filter((key)=>!adminNotificationReadState.has(key) && !adminNotificationResolvedState.has(key) && !adminNotificationArchivedState.has(key)).length+adminNotificationUnknownUnreadCount;
    document.getElementById("notifications-current").textContent=String(unresolved.length+adminNotificationUnknownUnreadCount);
    document.getElementById("notifications-unread").textContent=String(unread);
    document.getElementById("notifications-resolved").textContent=String([...adminCurrentNotificationKeys].filter((key)=>adminNotificationResolvedState.has(key) && !adminNotificationArchivedState.has(key)).length);
    const today=new Date().toDateString();
    document.getElementById("notifications-today").textContent=String(adminNotificationItems.filter((item)=>item.timestamp && new Date(item.timestamp).toDateString() === today).length);
    const rows=page.visible.map((item)=>{
      const row=document.createElement("article"); row.className=`card notification-center-item${adminNotificationReadState.has(item.key) ? " read" : ""}`;
      const copy=document.createElement("button"); copy.type="button"; copy.className="admin-notification-open"; copy.append(text("strong",item.title),text("small",item.meta));
      copy.addEventListener("click",()=>{ markAdminNotificationRead(item.key); item.open(); });
      const mark=document.createElement("button"); mark.type="button"; mark.className="admin-notification-mark"; mark.textContent=adminNotificationReadState.has(item.key) ? "Прочитано" : "Отметить прочитанным"; mark.disabled=adminNotificationReadState.has(item.key); mark.addEventListener("click",()=>markAdminNotificationRead(item.key));
      const resolve=document.createElement("button"); resolve.type="button"; resolve.className="admin-notification-mark"; const isResolved=adminNotificationResolvedState.has(item.key); resolve.textContent=isResolved ? "Открыть снова" : "Решено"; resolve.addEventListener("click",()=>updateAdminNotificationLifecycle(item.key,isResolved ? "reopen" : "resolve"));
      const archive=document.createElement("button"); archive.type="button"; archive.className="admin-notification-mark"; archive.textContent="Архив"; archive.addEventListener("click",()=>updateAdminNotificationLifecycle(item.key,"archive"));
      row.append(copy,mark,resolve,archive); return row;
    });
    if (!rows.length) rows.push(text("p","Уведомлений по выбранному фильтру нет.","card admin-panel-state"));
    if (adminNotificationPaginationIncomplete) rows.push(text("p","Не все уведомления удалось загрузить.","card admin-panel-state warning"));
    host.replaceChildren(...rows);
    const more=document.getElementById("notifications-page-more"); more.hidden=!page.remaining; more.textContent=`Показать ещё ${Math.min(MAX_NOTIFICATION_PANEL_ITEMS,page.remaining)}`;
  };
  const loadNotifications = () => {
    adminNotificationPageLimit=MAX_NOTIFICATION_PANEL_ITEMS; showScreen("notifications");
    return refreshAttentionCount();
  };
  const analyticsDelta = (current,previous) => {
    const difference=Number(current || 0)-Number(previous || 0);
    return difference === 0 ? "без изменений" : `${difference > 0 ? "+" : ""}${difference} к прошлому периоду`;
  };
  const loadAnalytics = () => {
    showScreen("analytics");
    const days=document.getElementById("analytics-period").value;
    return api(`/api/admin/analytics?days=${encodeURIComponent(days)}`).then((data)=>{
      const cards=[
        ["Всего участников",data.metrics.total_users_now,"total_users_now"],
        ["Активные участники",data.metrics.active_paid_now,"active_paid_now"],
        ["Новые участники",data.metrics.new_registrations,"new_registrations"],
        ["Завершили доступ",data.metrics.access_closed,"access_closed"],
      ].map(([label,value,key])=>{ const card=document.createElement("article"); card.className="card admin-kpi"; card.append(text("strong",String(value)),text("small",label),text("span",analyticsDelta(value,data.comparison[key]),"analytics-delta")); return card; });
      document.getElementById("analytics-summary").replaceChildren(...cards);
      const billing=[["Успешные оплаты","successful_payments"],["Новые подписки","initial_purchases"],["Продления","recurring_payments"],["Ошибки оплаты","failed_payments"],["Восстановлены","recovered_after_failure"],["Отмены автопродления","auto_renew_disabled"]];
      document.getElementById("analytics-billing").replaceChildren(...billing.map(([label,key])=>{ const row=document.createElement("div"); row.append(text("span",label),text("strong",String(data.metrics[key])),text("small",analyticsDelta(data.metrics[key],data.comparison[key]))); return row; }));
      const tracking=document.getElementById("analytics-tracking"); tracking.replaceChildren(text("p","Просмотры, старты, завершения, активные минуты и DAU/WAU/MAU пока не собираются. Здесь не отображаются фиктивные значения.","hint"));
    });
  };
  const configureAdminProfile = (identityData) => {
    const telegramUser = webApp.initDataUnsafe && webApp.initDataUnsafe.user ? webApp.initDataUnsafe.user : {};
    const confirmedProfile = identityData.profile || {};
    const profile = adminProfilePresentation({...telegramUser,first_name:confirmedProfile.first_name || telegramUser.first_name,username:confirmedProfile.username || telegramUser.username});
    const renderAvatar = (element) => {
      if (!element) return;
      element.replaceChildren(); element.textContent=profile.initials;
      if (!profile.photoUrl) return;
      const image=document.createElement("img"); image.alt=""; image.decoding="async"; image.referrerPolicy="no-referrer";
      image.addEventListener("error",()=>{ element.replaceChildren(); element.textContent=profile.initials; },{once:true});
      image.src=profile.photoUrl; element.replaceChildren(image);
    };
    const displayName = profile.displayName;
    document.getElementById("admin-dashboard-greeting").textContent=`Добрый день, ${displayName}!`;
    document.getElementById("admin-profile-name").textContent=displayName;
    document.getElementById("admin-profile-panel-name").textContent=displayName;
    renderAvatar(document.getElementById("admin-topbar-avatar"));
    renderAvatar(document.getElementById("admin-profile-panel-avatar"));
    document.getElementById("admin-profile-meta").textContent=confirmedProfile.username ? `@${confirmedProfile.username}` : `Telegram ID ${identityData.telegram_id}`;
    document.getElementById("admin-settings-name").textContent=displayName;
    document.getElementById("admin-settings-meta").textContent=confirmedProfile.username ? `@${confirmedProfile.username}` : `Telegram ID ${identityData.telegram_id}`;
    renderAvatar(document.getElementById("admin-settings-avatar"));
  };
  installAdminIcons();
  installMemberAdminCreateActions();
  if (!webApp || !webApp.initData) {
    status.textContent = "Мини-приложение пока доступно только администраторам.";
    identity.hidden = true;
    return;
  }
  webApp.ready();
  webApp.expand();
  let adminModeConfirmed = false;
  const mobileTelegramPlatforms = new Set(["android", "android_x", "ios"]);
  const showFullscreenFailure = (error) => {
    if (!fullscreenMessage) return;
    fullscreenMessage.textContent = "Полноэкранный режим недоступен в этой версии Telegram.";
    fullscreenMessage.hidden = false;
    const category = typeof error === "string" ? error : error && (error.error || error.error_type);
    if (String(category || "").toUpperCase() === "UNSUPPORTED") adminFullscreen.hidden = true;
  };
  const updateFullscreenControl = () => {
    if (!adminFullscreen) return;
    adminFullscreen.hidden = !adminModeConfirmed || Boolean(webApp.isFullscreen)
      || typeof webApp.requestFullscreen !== "function"
      || mobileTelegramPlatforms.has(String(webApp.platform || "").toLowerCase());
  };
  window.addEventListener("resize", updateFullscreenControl);
  if (webApp.onEvent) {
    webApp.onEvent("fullscreenChanged", () => {
      if (fullscreenMessage) fullscreenMessage.hidden = true;
      updateFullscreenControl();
      window.requestAnimationFrame(updateFullscreenControl);
    });
    webApp.onEvent("fullscreenFailed", showFullscreenFailure);
  }
  adminFullscreen.addEventListener("click", () => {
    if (typeof webApp.requestFullscreen !== "function") { adminFullscreen.hidden = true; return; }
    try {
      const result = webApp.requestFullscreen();
      if (result && typeof result.catch === "function") result.catch(showFullscreenFailure);
    } catch (_error) { showFullscreenFailure(); }
  });
  adminGlobalSearch.addEventListener("input", () => {
    window.clearTimeout(adminSearchTimer);
    adminSearchTimer=window.setTimeout(runAdminGlobalSearch,250);
  });
  adminGlobalSearch.addEventListener("focus", () => { if (adminGlobalSearch.value.trim()) runAdminGlobalSearch(); });
  adminGlobalSearch.addEventListener("keydown", (event) => {
    if (event.key === "Escape") { adminSearchResults.hidden=true; adminGlobalSearch.blur(); return; }
    const results=[...adminSearchResults.querySelectorAll("button")];
    if (!results.length || !["ArrowDown","ArrowUp","Enter"].includes(event.key)) return;
    if (event.key === "Enter" && document.activeElement !== adminGlobalSearch) return;
    event.preventDefault();
    if (event.key === "Enter") results[0].click();
    else results[event.key === "ArrowDown" ? 0 : results.length-1].focus();
  });
  adminSearchResults.addEventListener("keydown", (event) => {
    const results=[...adminSearchResults.querySelectorAll("button")]; const index=results.indexOf(document.activeElement);
    if (event.key === "Escape") { adminSearchResults.hidden=true; adminGlobalSearch.focus(); }
    else if (event.key === "ArrowDown" || event.key === "ArrowUp") { event.preventDefault(); results[(index+(event.key === "ArrowDown" ? 1 : -1)+results.length)%results.length].focus(); }
  });
  adminNotifications.addEventListener("click", () => {
    const opening=adminNotificationPanel.hidden; closeAdminHeaderPanels(adminNotificationPanel);
    adminNotificationPanel.hidden=!opening; adminNotifications.setAttribute("aria-expanded",String(opening));
    if (opening) { adminNotificationDisplayLimit=MAX_NOTIFICATION_PANEL_ITEMS; refreshAttentionCount().catch(showApiError); }
  });
  adminProfileToggle.addEventListener("click", () => {
    const opening=adminProfilePanel.hidden; closeAdminHeaderPanels(adminProfilePanel);
    adminProfilePanel.hidden=!opening; adminProfileToggle.setAttribute("aria-expanded",String(opening));
  });
  document.addEventListener("click", (event) => {
    if (!event.target.closest(".admin-global-search-wrap,.admin-header-popover")) closeAdminHeaderPanels();
  });
  adminContentNav.addEventListener("click", () => {
    const opening=adminContentSubnav.hidden;
    adminContentSubnav.hidden=!opening; adminContentNav.setAttribute("aria-expanded",String(opening));
  });
  adminContentSubnav.querySelectorAll("[data-content-nav]").forEach((button) => {
    button.addEventListener("click", () => guardContentNavigation(() => {
      contentType.value=button.dataset.contentNav === "all" ? "all" : button.dataset.contentNav;
      cmsContentStatus="all";
      contentStatusFilters.querySelectorAll("[data-content-status]").forEach((node)=>node.classList.toggle("active",node.dataset.contentStatus === "all"));
      adminContentSubnav.querySelectorAll("[data-content-nav]").forEach((node)=>node.classList.toggle("active",node===button));
      loadContent().catch(showApiError);
    }));
  });
  document.querySelectorAll("[data-nav]").forEach((button) => {
    button.addEventListener("click", () => {
      guardContentNavigation(() => {
        if (button.dataset.nav === "overview") loadDashboard().catch(showApiError);
        else if (button.dataset.nav === "users") loadUsers().catch(showApiError);
        else if (button.dataset.nav === "subscriptions") loadSubscriptions().catch(showApiError);
        else if (button.dataset.nav === "system") loadSystem().catch(showApiError);
        else if (button.dataset.nav === "analytics") loadAnalytics().catch(showApiError);
        else if (button.dataset.nav === "notifications") loadNotifications().catch(showApiError);
        else if (button.dataset.nav === "settings") showScreen("settings");
        else if (button.dataset.nav === "schedule") loadSchedule().catch(showApiError);
        else if (button.dataset.nav === "content") loadContent().catch(showApiError);
        else if (button.dataset.nav === "more") showScreen("more");
        else showScreen(button.dataset.nav);
      });
    });
  });
  refresh.addEventListener("click", () => {
    const reload = activeAdminScreen === "content" ? loadContent
      : activeAdminScreen === "users" ? loadUsers
      : activeAdminScreen === "subscriptions" ? loadSubscriptions
      : activeAdminScreen === "schedule" ? loadSchedule
      : activeAdminScreen === "gifts" ? loadGifts
      : activeAdminScreen === "failed-subscriptions" ? () => loadFailedSubscriptions(false)
      : activeAdminScreen === "system" ? loadSystem : loadDashboard;
    guardContentNavigation(() => reload().catch(showApiError));
  });
  document.getElementById("open-gifts").addEventListener("click", () => loadGifts().catch(showApiError));
  document.getElementById("open-subscriptions-mobile").addEventListener("click", () => loadSubscriptions().catch(showApiError));
  document.getElementById("open-schedule-mobile").addEventListener("click", () => loadSchedule().catch(showApiError));
  document.getElementById("open-content-legacy").addEventListener("click", () => loadContent().catch(showApiError));
  document.getElementById("open-content").addEventListener("click", () => loadContent().catch(showApiError));
  document.getElementById("topbar-create-content").addEventListener("click", () => { showScreen("content-create"); loadTaxonomy(contentCreateType.value, contentCreateTaxonomy).catch(showApiError); });
  const openAdminClub = () => {
    adminScreenBeforeClub = activeAdminScreen;
    return loadMemberHome().catch(showApiError);
  };
  document.getElementById("open-member-preview").addEventListener("click", openAdminClub);
  document.getElementById("open-club-global").addEventListener("click", () => guardContentNavigation(openAdminClub));
  document.getElementById("sidebar-open-club").addEventListener("click", () => guardContentNavigation(openAdminClub));
  document.getElementById("dashboard-open-users").addEventListener("click", () => loadUsers().catch(showApiError));
  document.getElementById("dashboard-open-system").addEventListener("click", () => loadSystem().catch(showApiError));
  document.getElementById("dashboard-open-attention").addEventListener("click", () => loadNotifications().catch(showApiError));
  document.getElementById("more-subscriptions").addEventListener("click", () => loadSubscriptions().catch(showApiError));
  document.getElementById("more-schedule").addEventListener("click", () => loadSchedule().catch(showApiError));
  document.getElementById("more-gifts").addEventListener("click", () => loadGifts().catch(showApiError));
  document.getElementById("more-failed-subscriptions").addEventListener("click", () => loadNotifications().catch(showApiError));
  document.getElementById("more-system").addEventListener("click", () => showScreen("settings"));
  document.getElementById("more-open-club").addEventListener("click", openAdminClub);
  document.getElementById("attention-back").addEventListener("click", () => loadDashboard().catch(showApiError));
  document.getElementById("open-failed-subscriptions").addEventListener("click", () => loadNotifications().catch(showApiError));
  document.getElementById("nav-gifts").addEventListener("click", () => loadGifts().catch(showApiError));
  document.getElementById("analytics-period").addEventListener("change", () => loadAnalytics().catch(showApiError));
  document.getElementById("notifications-category").addEventListener("change", () => { adminNotificationPageLimit=MAX_NOTIFICATION_PANEL_ITEMS; renderNotificationCenter(); });
  document.getElementById("notifications-read-filter").addEventListener("change", () => { adminNotificationPageLimit=MAX_NOTIFICATION_PANEL_ITEMS; renderNotificationCenter(); });
  document.getElementById("notifications-resolution-filter").addEventListener("change", () => { adminNotificationPageLimit=MAX_NOTIFICATION_PANEL_ITEMS; renderNotificationCenter(); });
  document.getElementById("notifications-mark-all-read").addEventListener("click", () => Promise.all(
    [...adminCurrentNotificationKeys].filter((key)=>!adminNotificationReadState.has(key)).map(markAdminNotificationRead)
  ).then(renderNotificationCenter));
  document.getElementById("notifications-page-more").addEventListener("click", () => { adminNotificationPageLimit += MAX_NOTIFICATION_PANEL_ITEMS; renderNotificationCenter(); });
  document.getElementById("settings-open-notifications").addEventListener("click", () => loadNotifications().catch(showApiError));
  document.getElementById("settings-open-system").addEventListener("click", () => loadSystem().catch(showApiError));
  document.getElementById("admin-profile-open").addEventListener("click", () => { closeAdminHeaderPanels(); showScreen("settings"); });
  document.getElementById("admin-settings-open").addEventListener("click", () => { closeAdminHeaderPanels(); showScreen("settings"); });
  document.getElementById("admin-session-exit").addEventListener("click", () => writeAdminJson("POST","/api/admin/session/revoke",{}).finally(()=>webApp.close()));
  failedSubscriptionsFilter.addEventListener("change", () => { failedSubscriptionsCursor=null; loadFailedSubscriptions(false); });
  failedSubscriptionsMore.addEventListener("click", () => loadFailedSubscriptions(true));
  document.getElementById("failed-subscriptions-dashboard-back").addEventListener("click", () => loadDashboard().catch(showApiError));
  document.getElementById("failed-subscriptions-back").addEventListener("click", () => loadFailedSubscriptions(false));
  document.getElementById("member-home-all").addEventListener("click", () => loadMemberLibrary().catch(showApiError));
  document.getElementById("member-home-library").addEventListener("click", () => loadMemberLibrary().catch(showApiError));
  document.getElementById("member-continue").addEventListener("click", () => {
    if (realMemberMode && !memberEntitled) webApp.close();
    else loadMemberLibrary().catch(showApiError);
  });
  document.getElementById("member-lesson-back").addEventListener("click", () => {
    const target = memberDetailContentType === "meditation" ? loadMemberMeditations()
      : memberDetailContentType === "recipe" ? loadMemberRecipes()
      : memberDetailContentType === "nutrition_material" ? loadMemberNutrition()
      : loadMemberLibrary(memberLibraryCategory);
    target.catch(showApiError);
  });
  document.getElementById("member-open-meditations").addEventListener("click", () => loadMemberMeditations().catch(showApiError));
  const debounceMemberRender=(render)=>()=>{
    window.clearTimeout(memberSearchTimer);
    memberSearchTimer=window.setTimeout(render,200);
  };
  memberMeditationSearch.addEventListener("input", debounceMemberRender(renderMemberMeditations));
  document.getElementById("member-open-recipes").addEventListener("click", () => loadMemberRecipes().catch(showApiError));
  document.getElementById("member-open-nutrition").addEventListener("click", () => loadMemberNutrition().catch(showApiError));
  memberNutritionSearch.addEventListener("input", debounceMemberRender(renderMemberNutrition));
  memberRecipeSearch.addEventListener("input", debounceMemberRender(renderMemberRecipes));
  document.getElementById("member-open-schedule").addEventListener("click", () => loadMemberSchedule().catch(showApiError));
  document.getElementById("class-create-toggle").addEventListener("click",()=>{ editingClassId=null; classCreateForm.hidden=!classCreateForm.hidden; if(!classCreateForm.hidden) classCreateForm.reset(); });
  document.getElementById("class-create-cancel").addEventListener("click",()=>{ editingClassId=null; classCreateForm.hidden=true; classCreateForm.reset(); });
  classCreateForm.addEventListener("submit",(event)=>{
    event.preventDefault();
    const price=Math.round(Number(document.getElementById("class-price").value)*100);
    const payload={title:document.getElementById("class-title").value,description:document.getElementById("class-description").value,starts_at:new Date(document.getElementById("class-start").value).toISOString(),duration_minutes:Number(document.getElementById("class-duration").value),zoom_url:document.getElementById("class-zoom-url").value,price_amount:price,capacity:Number(document.getElementById("class-capacity").value),minimum_participants:Number(document.getElementById("class-minimum").value),booking_deadline:new Date(document.getElementById("class-deadline").value).toISOString()};
    const method=editingClassId ? "PUT" : "POST"; const path=editingClassId ? `/api/admin/classes/${encodeURIComponent(editingClassId)}` : "/api/admin/classes";
    writeAdminJson(method,path,payload).then(()=>{ editingClassId=null; classCreateForm.hidden=true; classCreateForm.reset(); return loadSchedule(false); }).catch((error)=>{ document.getElementById("class-create-message").textContent=`Не удалось сохранить: ${error.message}`; });
  });
  document.getElementById("member-library-empty-home").addEventListener("click", () => loadMemberHome().catch(showApiError));
  document.querySelectorAll("[data-member-category-nav]").forEach((button) => {
    button.addEventListener("click", () => loadMemberLibrary(button.dataset.memberCategoryNav).catch(showApiError));
  });
  memberLibrarySearch.addEventListener("input", debounceMemberRender(renderMemberLibrary));
  window.addEventListener("pagehide", clearMemberCoverUrls, {once:true});
  document.querySelectorAll(".member-exit").forEach((button) => {
    button.addEventListener("click", () => exitMemberPreview().catch(showApiError));
  });
  document.querySelectorAll("[data-member-nav]").forEach((button) => {
    button.addEventListener("click", () => {
      if (button.dataset.memberNav === "member-home") loadMemberHome().catch(showApiError);
      else if (button.dataset.memberNav === "member-library") loadMemberLibrary().catch(showApiError);
      else if (button.dataset.memberNav === "member-schedule") loadMemberSchedule().catch(showApiError);
      else if (button.dataset.memberNav === "member-profile") loadMemberProfile().catch(showApiError);
      else showMemberScreen(button.dataset.memberNav);
    });
  });
  document.getElementById("content-create-open").addEventListener("click", () => { showScreen("content-create"); loadTaxonomy(contentCreateType.value, contentCreateTaxonomy).catch(showApiError); });
  document.getElementById("content-editor-new").addEventListener("click", () => guardContentNavigation(()=>{ showScreen("content-create"); loadTaxonomy(contentCreateType.value, contentCreateTaxonomy).catch(showApiError); }));
  document.querySelectorAll("[data-preview-mode]").forEach((button)=>button.addEventListener("click",()=>{
    document.querySelectorAll("[data-preview-mode]").forEach((candidate)=>candidate.classList.toggle("active",candidate===button));
    contentLivePreview.dataset.previewMode=button.dataset.previewMode;
  }));
  document.getElementById("content-create-back").addEventListener("click", () => loadContent().catch(showApiError));
  document.getElementById("content-create-submit").addEventListener("click", createCmsDraft);
  document.getElementById("content-create-publish").addEventListener("click", () => {
    createCmsDraft().then((item) => item ? previewContentLifecycle() : null).catch(() => null);
  });
  contentCreateType.addEventListener("change", () => loadTaxonomy(contentCreateType.value, contentCreateTaxonomy).catch(showApiError));
  document.getElementById("content-edit-save").addEventListener("click", saveCurrentContent);
  const showContentPreviewPanel = () => {
    renderContentLivePreview();
    contentStudioWorkspace.classList.add("preview-active");
    contentStudioTabs.querySelectorAll("[data-studio-panel]").forEach((button) => button.classList.toggle("active", button.dataset.studioPanel === "preview"));
    contentLivePreview.scrollIntoView({behavior: "smooth", block: "start"});
  };
  const runContentPublishAction = () => {
    if (!currentCmsContent) return;
    if (currentCmsContent.status === "draft" && !contentEditorDirty) previewContentLifecycle();
    else if (currentCmsContent.status === "published") previewContentLifecycle();
  };
  contentBottomSave.addEventListener("click", saveCurrentContent);
  contentBottomPublish.addEventListener("click", runContentPublishAction);
  contentEditorMore.addEventListener("click", () => {
    contentEditorMenu.hidden = !contentEditorMenu.hidden;
    contentEditorMore.setAttribute("aria-expanded", String(!contentEditorMenu.hidden));
  });
  document.getElementById("content-menu-preview").addEventListener("click", () => { contentEditorMenu.hidden = true; showContentPreviewPanel(); });
  document.getElementById("content-menu-history").addEventListener("click", () => { contentEditorMenu.hidden = true; document.getElementById("content-version-history-card").open = true; document.getElementById("content-version-history-card").scrollIntoView({behavior:"smooth",block:"start"}); });
  document.getElementById("content-history-close").addEventListener("click", () => { document.getElementById("content-version-history-card").open = false; });
  document.getElementById("content-menu-technical").addEventListener("click", () => { contentEditorMenu.hidden = true; status.textContent = currentCmsContent ? `Версия данных ${currentCmsContent.version} · ревизия ${currentCmsContent.revision_number}` : "Технические сведения недоступны"; });
  contentCreateRevision.addEventListener("click", createContentRevision);
  document.querySelectorAll(".authoring-textarea").forEach((field) => {
    const grow = () => { field.style.height = "auto"; field.style.height = `${Math.max(160, field.scrollHeight)}px`; };
    field.addEventListener("input", grow);
  });
  document.getElementById("recipe-add-ingredient").addEventListener("click", () => { if (recipeIngredients.length < 100) { recipeIngredients.push({name: "", amount: ""}); setContentEditorDirty(true); renderRecipeEditor(); } });
  document.getElementById("recipe-add-step").addEventListener("click", () => { if (recipeSteps.length < 50) { recipeSteps.push({instruction: ""}); setContentEditorDirty(true); renderRecipeEditor(); } });
  document.getElementById("recipe-save").addEventListener("click", saveCurrentContent);
  document.getElementById("content-nutrition-save").addEventListener("click", saveCurrentContent);
  contentCreateType.addEventListener("change", () => {
    const nutrition = contentCreateType.value === "nutrition_material";
    const recipe = contentCreateType.value === "recipe";
    const meditation = contentCreateType.value === "meditation";
    contentCreateBodyLabel.hidden = !nutrition;
    contentCreateRecipeFields.hidden = !recipe;
    contentCreateDuration.closest("label").hidden = nutrition;
    contentCreateVideoLabel.hidden = recipe || nutrition;
    contentCreateAudioLabel.hidden = !meditation;
  });
  document.getElementById("content-cover-validate").addEventListener("click", () => validateContentMedia("cover"));
  document.getElementById("content-video-validate").addEventListener("click", () => validateContentMedia("video"));
  document.getElementById("content-audio-validate").addEventListener("click", () => validateContentMedia("audio"));
  contentCoverFile.addEventListener("change", () => showLocalContentMedia("cover"));
  contentVideoFile.addEventListener("change", () => showLocalContentMedia("video"));
  contentAudioFile.addEventListener("change", () => showLocalContentMedia("audio"));
  contentMediaConfirm.addEventListener("click", confirmContentMedia);
  contentMediaCancel.addEventListener("click", cancelContentMedia);
  contentLifecyclePreviewButton.addEventListener("click", previewContentLifecycle);
  contentLifecycleConfirm.addEventListener("click", confirmContentLifecycle);
  contentLifecycleCancel.addEventListener("click", cancelContentLifecycle);
  usersMore.addEventListener("click", () => loadUsers(true).catch(showApiError));
  const syncUsersQuickFilters = () => document.querySelectorAll("[data-users-status]").forEach((button) => {
    button.classList.toggle("active", button.dataset.usersStatus === usersStatus.value);
  });
  usersStatus.addEventListener("change", () => { syncUsersQuickFilters(); loadUsers().catch(showApiError); });
  document.querySelectorAll("[data-users-status]").forEach((button) => button.addEventListener("click", () => {
    usersStatus.value = button.dataset.usersStatus;
    syncUsersQuickFilters();
    loadUsers().catch(showApiError);
  }));
  document.getElementById("users-search-focus").addEventListener("click", () => usersSearch.focus());
  usersSearch.addEventListener("input", () => {
    window.clearTimeout(searchTimer);
    searchTimer = window.setTimeout(() => loadUsers().catch(showApiError), 300);
  });
  document.getElementById("users-back").addEventListener("click", () => {
    showScreen("users");
    window.requestAnimationFrame(() => window.scrollTo({top:usersListScrollPosition,behavior:"auto"}));
  });
  document.getElementById("user-more-actions").addEventListener("click", () => selectUserProfileTab("club"));
  document.querySelectorAll("[data-user-tab]").forEach((tab) => {
    tab.addEventListener("click", () => selectUserProfileTab(tab.dataset.userTab));
  });
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
  contentType.addEventListener("change", () => loadContent().catch(showApiError));
  contentStatusFilters.addEventListener("click", (event) => {
    const button = event.target.closest("[data-content-status]");
    if (!button) return;
    cmsContentStatus = button.dataset.contentStatus;
    contentStatusFilters.querySelectorAll("[data-content-status]").forEach((node) => node.classList.toggle("active", node === button));
    loadContent().catch(showApiError);
  });
  contentSearch.addEventListener("input", () => {
    window.clearTimeout(contentSearchTimer);
    contentSearchTimer = window.setTimeout(() => loadContent().catch(showApiError), 300);
  });
  document.getElementById("content-back").addEventListener("click", () => guardContentNavigation(() => loadContent().catch(showApiError)));
  document.getElementById("content-dashboard-back").addEventListener("click", () => loadDashboard().catch(showApiError));
  [contentEditTitle, contentEditDescription, contentEditDuration, contentEditOrder, contentEditAccess].forEach((field) => {
    field.addEventListener("input", () => { setContentEditorDirty(true); renderContentLivePreview(); });
  });
  contentEditTaxonomy.addEventListener("change", () => { setContentEditorDirty(true); renderContentLivePreview(); });
  contentNutritionBody.addEventListener("input", () => { setContentEditorDirty(true); renderContentLivePreview(); });
  contentStudioTabs.addEventListener("click", (event) => {
    const button = event.target.closest("[data-studio-panel]");
    if (!button) return;
    const preview = button.dataset.studioPanel === "preview";
    contentStudioWorkspace.classList.toggle("preview-active", preview);
    contentStudioTabs.querySelectorAll("[data-studio-panel]").forEach((node) => node.classList.toggle("active", node === button));
    if (preview) renderContentLivePreview();
  });
  document.getElementById("content-unsaved-save").addEventListener("click", () => {
    saveCurrentContent().then(runPendingContentNavigation).catch(() => null);
  });
  document.getElementById("content-unsaved-discard").addEventListener("click", () => { setContentEditorDirty(false); runPendingContentNavigation(); });
  document.getElementById("content-unsaved-cancel").addEventListener("click", () => { pendingContentNavigation = null; contentUnsavedDialog.close(); });
  fetch("/api/admin/session", {
    method: "POST", headers: {Authorization: `tma ${webApp.initData}`},
    cache: "no-store", credentials: "omit",
  }).then((response) => {
    if (response.ok) return response.json().then((session) => ({session,admin:true}));
    if (response.status !== 403) throw new Error("telegram_session_expired");
    return fetch("/api/member/auth", {method:"POST",headers:{Authorization:`tma ${webApp.initData}`},cache:"no-store",credentials:"omit"}).then((memberResponse) => {
      if (memberResponse.status === 403) throw new Error("member_rollout_disabled");
      if (!memberResponse.ok) throw new Error("telegram_session_expired");
      return memberResponse.json().then((session) => ({session,admin:false}));
    });
  }).then(({session,admin}) => {
    sessionToken=session.token;
    if (admin) return api("/api/admin/me").then((identityData) => ({admin:true,identityData}));
    realMemberMode=true; memberEntitled=Boolean(session.access && session.access.has_active_access);
    document.getElementById("member-greeting").textContent=session.profile && session.profile.first_name ? `Здравствуйте, ${session.profile.first_name}!` : "Здравствуйте!";
    const profileName=document.getElementById("member-profile-name");
    if (profileName) profileName.textContent=session.profile && session.profile.first_name ? session.profile.first_name : "Профиль";
    syncMemberAccess(session.access);
    return {admin:false,identityData:null};
  }).then(({admin,identityData}) => {
    if (!admin) return loadMemberHome();
    adminModeConfirmed = true;
    configureAdminProfile(identityData);
    updateFullscreenControl();
    console.info("MINIAPP_VIEWPORT_DIAGNOSTIC", {
      platform: String(webApp.platform || "unknown"),
      innerWidth: window.innerWidth,
      innerHeight: window.innerHeight,
      viewportWidth: typeof webApp.viewportWidth === "number" ? webApp.viewportWidth : null,
      viewportHeight: typeof webApp.viewportHeight === "number" ? webApp.viewportHeight : null,
      isFullscreen: Boolean(webApp.isFullscreen),
      requestFullscreenSupported: typeof webApp.requestFullscreen === "function",
    });
    telegramId.textContent=String(identityData.telegram_id); identity.hidden=false; bottomNav.hidden=false;
    return loadDashboard().then(() => {
      window.setTimeout(() => {
        loadAdminNotificationAcknowledgements()
          .then(refreshAttentionCount)
          .then(({failed,gifts}) => {
            renderDashboardGifts({...gifts,items:(gifts.items || []).slice(0,4)});
            renderDashboardFailures({...failed,items:(failed.items || []).slice(0,4)});
          })
          .catch(showApiError);
      }, 0);
    });
  }).catch((error) => {
    if (error.message === "member_rollout_disabled") { status.textContent="Новая платформа пока доступна только участникам тестирования."; identity.hidden=true; return; }
    if (error.message === "telegram_session_expired") {
      status.textContent = "Сессия Telegram устарела. Закройте и откройте мини-приложение снова.";
      identity.hidden = true;
      return;
    }
    showApiError(error);
  });
})();
