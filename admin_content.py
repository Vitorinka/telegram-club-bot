from copy import deepcopy


MAX_CONTENT_QUERY_LENGTH = 80
CONTENT_CATEGORIES = frozenset({
    "all", "onboarding", "club_information", "free_materials",
})


class AdminContentQueryError(ValueError):
    pass


def _catalog(free_lesson_configured):
    return (
        {
            "content_id": "onboarding-welcome",
            "content_type": "onboarding_step",
            "title": "Приветствие клуба",
            "category": "onboarding",
            "category_label": "Онбординг",
            "short_description": "Первое приветственное сообщение при регистрации.",
            "duration_minutes": None,
            "ordering": 1,
            "media_type": "photo",
            "has_media": True,
            "availability": "configured",
            "published": None,
            "active": None,
            "created_at": None,
            "updated_at": None,
        },
        {
            "content_id": "onboarding-club-overview",
            "content_type": "onboarding_step",
            "title": "Знакомство с клубом",
            "category": "onboarding",
            "category_label": "Онбординг",
            "short_description": "Видео о клубе и доступных участникам материалах.",
            "duration_minutes": None,
            "ordering": 2,
            "media_type": "video",
            "has_media": True,
            "availability": "configured",
            "published": None,
            "active": None,
            "created_at": None,
            "updated_at": None,
        },
        {
            "content_id": "onboarding-faq",
            "content_type": "onboarding_step",
            "title": "FAQ перед вступлением",
            "category": "onboarding",
            "category_label": "Онбординг",
            "short_description": "Текстовый шаг регистрации с ответами на частые вопросы.",
            "duration_minutes": None,
            "ordering": 3,
            "media_type": "text",
            "has_media": False,
            "availability": "configured",
            "published": None,
            "active": None,
            "created_at": None,
            "updated_at": None,
        },
        {
            "content_id": "club-rules",
            "content_type": "rules",
            "title": "Правила клуба",
            "category": "club_information",
            "category_label": "Информация клуба",
            "short_description": "Правила, доступные из главного меню бота.",
            "duration_minutes": None,
            "ordering": None,
            "media_type": "text",
            "has_media": False,
            "availability": "configured",
            "published": None,
            "active": None,
            "created_at": None,
            "updated_at": None,
        },
        {
            "content_id": "free-lesson",
            "content_type": "free_lesson",
            "title": "Бесплатный урок",
            "category": "free_materials",
            "category_label": "Бесплатные материалы",
            "short_description": "Бесплатная видеотренировка, доступная из главного меню.",
            "duration_minutes": 15,
            "ordering": 1,
            "media_type": "video",
            "has_media": bool(free_lesson_configured),
            "availability": (
                "configured" if free_lesson_configured else "missing_media"
            ),
            "published": None,
            "active": None,
            "created_at": None,
            "updated_at": None,
        },
        {
            "content_id": "free-lesson-followup",
            "content_type": "followup",
            "title": "Напоминание после бесплатного урока",
            "category": "free_materials",
            "category_label": "Бесплатные материалы",
            "short_description": "Текстовое сообщение после бесплатного урока.",
            "duration_minutes": None,
            "ordering": 2,
            "media_type": "text",
            "has_media": False,
            "availability": "configured",
            "published": None,
            "active": None,
            "created_at": None,
            "updated_at": None,
        },
    )


def _validate_category(value):
    category = str(value or "all")
    if category not in CONTENT_CATEGORIES:
        raise AdminContentQueryError("invalid_category")
    return category


def _validate_query(value):
    query = str(value or "").strip()
    if len(query) > MAX_CONTENT_QUERY_LENGTH:
        raise AdminContentQueryError("query_too_long")
    return query.casefold()


def list_admin_content(*, free_lesson_configured, category="all", query=""):
    category = _validate_category(category)
    query = _validate_query(query)
    catalog = _catalog(free_lesson_configured)
    items = [
        deepcopy(item) for item in catalog
        if (category == "all" or item["category"] == category)
        and (not query or query in item["title"].casefold())
    ]
    category_counts = {
        key: sum(item["category"] == key for item in catalog)
        for key in sorted(CONTENT_CATEGORIES - {"all"})
    }
    return {
        "items": items,
        "summary": {"total": len(catalog), "categories": category_counts},
        "filters": [
            {"value": "all", "label": "Все"},
            {"value": "onboarding", "label": "Онбординг"},
            {"value": "club_information", "label": "Информация клуба"},
            {"value": "free_materials", "label": "Бесплатные материалы"},
        ],
        "read_only": True,
    }


def get_admin_content(content_id, *, free_lesson_configured):
    content_id = str(content_id or "")
    for item in _catalog(free_lesson_configured):
        if item["content_id"] == content_id:
            result = deepcopy(item)
            result["read_only"] = True
            return result
    return None
