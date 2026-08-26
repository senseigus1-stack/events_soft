export type Language = "ru" | "en" | "zh" | "udm";

export const LANGUAGES: { id: Language; label: string; short: string }[] = [
  { id: "ru", label: "Русский", short: "RU" },
  { id: "en", label: "English", short: "EN" },
  { id: "zh", label: "中文", short: "中文" },
  { id: "udm", label: "Удмурт кыл", short: "УДМ" },
];

export const LOCALES: Record<Language, string> = {
  ru: "ru-RU",
  en: "en-GB",
  zh: "zh-CN",
  udm: "udm-RU",
};

const ru = {
  brandHome: "Ваёбыж — на главную", kytchi: "Кытчи", plans: "Планы", friends: "Друзья",
  talks: "Обсуждения", ownEvent: "своё событие", signIn: "войти", account: "аккаунт", language: "Язык",
  heroSwipe: "смахни", heroBoredom: "скуку.", heroKicker: "Кытчи / персональная подборка",
  heroLead: "Не афиша. Живой радар того, что может стать вашим вечером.", city: "Город", yourCity: "Ваш город",
  kytchiEyebrow: "01 / КЫТЧИ", swipeTitle: "Вы решаете жестом. Кытчи запоминает смысл.",
  swipeBody: "Влево — не ваше. Вправо или тап — подробности. «Пойду» усиливает похожие рекомендации и включает напоминание.",
  tune: "настроить интересы", demo: "демо-каталог · API подключается автоматически", loading: "Кытчи собирает вашу ленту",
  notMine: "НЕ МОЁ", look: "СМОТРЮ", details: "подробнее", dislike: "Не интересно", going: "Пойду",
  feedDone: "Лента закончилась. Хороший знак.", feedLearned: "Кытчи уже понял ваши свайпы.", again: "Посмотреть ещё раз",
  signals: "ВАШИ СИГНАЛЫ", whatInterests: "Что сейчас цепляет?", rebuild: "пересобрать ленту",
  plansEyebrow: "02 / ПЛАНЫ", plansTitle: "Пойти — это уже план.",
  plansBody: "За сутки до события «Ваёбыж» напомнит внутри приложения. Позже сюда легко подключаются Telegram и push.",
  plansEmpty: "Нажмите сердце на карточке — первый план появится здесь.",
  peopleEyebrow: "03 / ЛЮДИ", peopleTitle: "Не гадать, кто куда.",
  peopleBody: "Друзья видят только отмеченные планы. Их выбор помогает Кытчи находить события, на которых вы реально встретитесь.",
  newFriend: "Новый друг", wantsFriend: "хочет добавить вас", requestSent: "запрос отправлен", noPlans: "пока без планов", accepts: "принять",
  addFriendTitle: "Добавить своего", addFriendBody: "Вставьте короткий код профиля. Без поиска по телефонной книге.", friendExample: "например, katya", friendCode: "Код друга", yourCode: "Ваш код",
  talksEyebrow: "04 / ОБСУЖДАЛКИ", talksTitle: "Разговор привязан к месту.", town: "ГОРОД", venue: "МЕСТО", event: "СОБЫТИЕ", messages: "сообщений", enter: "зайти",
  communityEyebrow: "СОБЫТИЯ ОТ ЛЮДЕЙ", communityTitle: "Устроили что-то настоящее?",
  communityBody: "Добавьте событие. После проверки администратором оно попадёт в общую ленту и в рекомендации тем, кому действительно подходит.",
  propose: "предложить событие", footerLine: "События находят вас. Рекомендации — Кытчи.",
  close: "Закрыть", kytchiSuggests: "КЫТЧИ ПРЕДЛАГАЕТ", whyYou: "Почему вам", when: "когда", where: "где", admission: "вход", free: "Бесплатно", sitePrice: "Цена на сайте", youGo: "Вы идёте", eventPage: "страница события",
  accepted: "ПРИНЯТО", sentTitle: "Событие ушло на проверку.", sentBody: "После решения администратора вы получите уведомление, а одобренная карточка появится в ленте.", understood: "понятно",
  submissionEyebrow: "СОБЫТИЕ ОТ СООБЩЕСТВА", submissionTitle: "Покажите городу, что вы придумали.", name: "Название", nameHint: "Ночь независимого кино", dateTime: "Дата и время", category: "Категория", description: "Что будет", descriptionHint: "Расскажите достаточно, чтобы модератор и будущие гости всё поняли.", venueName: "Площадка", venueHint: "Название места", address: "Адрес", addressHint: "Улица, дом", tags: "Теги через запятую", tagsHint: "кино, встреча, вечер", price: "Цена", priceHint: "от 500 ₽", freeEntry: "Вход бесплатный", sending: "отправляем…", sendModeration: "отправить на модерацию",
  loginTitle: "Войти в Ваёбыж", loginBody: "Выберите привычный сервис. Пароль от него остаётся у сервиса, а ваши свайпы и планы сохранятся в Ваёбыже.", guestMode: "Сейчас вы гость", providerUnavailable: "Провайдер ещё не настроен на сервере", loginError: "Не удалось начать вход. Попробуйте ещё раз.", logout: "выйти", udmurtBeta: "Удмуртская версия — бета; поможете уточнить формулировки?",
  music: "Музыка", exhibitions: "Выставки", theatre: "Театр", cinema: "Кино", lectures: "Лекции", festivals: "Фестивали", kids: "С детьми", sport: "Спорт", other: "Другое",
} as const;

type Copy = { [K in keyof typeof ru]: string };

const en: Copy = {
  brandHome: "Vayobyzh — home", kytchi: "Kytchi", plans: "Plans", friends: "Friends", talks: "Discussions", ownEvent: "add event", signIn: "sign in", account: "account", language: "Language",
  heroSwipe: "swipe", heroBoredom: "boredom.", heroKicker: "Kytchi / personal picks", heroLead: "Not a listing. A live radar for what could become your evening.", city: "City", yourCity: "Your city",
  kytchiEyebrow: "01 / KYTCHI", swipeTitle: "You decide with a gesture. Kytchi remembers the meaning.", swipeBody: "Left means not for you. Right or tap opens details. Going strengthens similar picks and sets a reminder.", tune: "tune interests", demo: "demo catalogue · API connects automatically", loading: "Kytchi is building your feed", notMine: "NOT MINE", look: "OPEN", details: "details", dislike: "Not interested", going: "I'm going", feedDone: "That’s the end of the feed. A good sign.", feedLearned: "Kytchi has learned from your swipes.", again: "Show again", signals: "YOUR SIGNALS", whatInterests: "What pulls you in now?", rebuild: "rebuild feed",
  plansEyebrow: "02 / PLANS", plansTitle: "Going is already a plan.", plansBody: "Vayobyzh reminds you in the app one day before. Telegram and push can be added next.", plansEmpty: "Tap the heart on a card — your first plan will appear here.", peopleEyebrow: "03 / PEOPLE", peopleTitle: "Know who is going where.", peopleBody: "Friends only see plans you marked. Their choices help Kytchi find events where you can actually meet.", newFriend: "New friend", wantsFriend: "wants to add you", requestSent: "request sent", noPlans: "no plans yet", accepts: "accept", addFriendTitle: "Add a friend", addFriendBody: "Paste their short profile code. No contacts upload.", friendExample: "for example, katya", friendCode: "Friend code", yourCode: "Your code",
  talksEyebrow: "04 / DISCUSSIONS", talksTitle: "Every conversation has a place.", town: "CITY", venue: "VENUE", event: "EVENT", messages: "messages", enter: "enter", communityEyebrow: "EVENTS BY PEOPLE", communityTitle: "Made something real?", communityBody: "Add your event. Once approved by an admin, it enters the feed and reaches the people it genuinely fits.", propose: "submit an event", footerLine: "Events find you. Recommendations by Kytchi.", close: "Close", kytchiSuggests: "KYTCHI SUGGESTS", whyYou: "Why it fits", when: "when", where: "where", admission: "entry", free: "Free", sitePrice: "Price on website", youGo: "You're going", eventPage: "event page",
  accepted: "RECEIVED", sentTitle: "Your event is under review.", sentBody: "You will be notified after the admin decision; approved events appear in the feed.", understood: "got it", submissionEyebrow: "COMMUNITY EVENT", submissionTitle: "Show the city what you made.", name: "Name", nameHint: "Independent film night", dateTime: "Date and time", category: "Category", description: "What will happen", descriptionHint: "Give future guests and the moderator enough useful detail.", venueName: "Venue", venueHint: "Venue name", address: "Address", addressHint: "Street and building", tags: "Comma-separated tags", tagsHint: "film, meetup, evening", price: "Price", priceHint: "from 500 ₽", freeEntry: "Free entry", sending: "sending…", sendModeration: "send for review",
  loginTitle: "Sign in to Vayobyzh", loginBody: "Choose a familiar service. Your password stays there, while your swipes and plans remain in Vayobyzh.", guestMode: "You are browsing as a guest", providerUnavailable: "This provider is not configured on the server yet", loginError: "Could not start sign-in. Please try again.", logout: "sign out", udmurtBeta: "The Udmurt version is in beta — help us refine it.", music: "Music", exhibitions: "Exhibitions", theatre: "Theatre", cinema: "Film", lectures: "Talks", festivals: "Festivals", kids: "With kids", sport: "Sport", other: "Other",
};

const zh: Copy = {
  brandHome: "Vayobyzh — 首页", kytchi: "Kytchi", plans: "计划", friends: "好友", talks: "讨论", ownEvent: "发布活动", signIn: "登录", account: "账户", language: "语言",
  heroSwipe: "划走", heroBoredom: "无聊。", heroKicker: "Kytchi / 个性化推荐", heroLead: "不只是活动列表，而是发现今晚可能发生什么的实时雷达。", city: "城市", yourCity: "你的城市",
  kytchiEyebrow: "01 / KYTCHI", swipeTitle: "一个手势做决定，Kytchi 记住你的偏好。", swipeBody: "左滑表示不喜欢；右滑或点击查看详情。“想去”会增强相似推荐并开启提醒。", tune: "调整兴趣", demo: "演示目录 · API 会自动连接", loading: "Kytchi 正在生成你的推荐", notMine: "不喜欢", look: "看看", details: "详情", dislike: "不感兴趣", going: "想去", feedDone: "推荐看完了，这是好事。", feedLearned: "Kytchi 已从你的滑动中学习。", again: "再看一次", signals: "你的信号", whatInterests: "现在什么最吸引你？", rebuild: "重新生成",
  plansEyebrow: "02 / 计划", plansTitle: "决定去，就有了计划。", plansBody: "Vayobyzh 会提前一天在应用内提醒，之后还可接入 Telegram 和推送。", plansEmpty: "点击卡片上的爱心，第一个计划会出现在这里。", peopleEyebrow: "03 / 好友", peopleTitle: "看看朋友都去哪。", peopleBody: "好友只能看到你标记的计划。他们的选择也帮助 Kytchi 找到真正能见面的活动。", newFriend: "新好友", wantsFriend: "想添加你", requestSent: "请求已发送", noPlans: "暂时没有计划", accepts: "接受", addFriendTitle: "添加好友", addFriendBody: "输入对方的简短个人代码，无需上传通讯录。", friendExample: "例如 katya", friendCode: "好友代码", yourCode: "你的代码",
  talksEyebrow: "04 / 讨论", talksTitle: "每段讨论都属于一个地方。", town: "城市", venue: "地点", event: "活动", messages: "条消息", enter: "进入", communityEyebrow: "用户活动", communityTitle: "你办了有趣的活动？", communityBody: "提交活动。管理员审核通过后，它会进入公共推荐流并触达真正感兴趣的人。", propose: "提交活动", footerLine: "活动找到你，Kytchi 负责推荐。", close: "关闭", kytchiSuggests: "KYTCHI 推荐", whyYou: "为什么适合你", when: "时间", where: "地点", admission: "入场", free: "免费", sitePrice: "价格见官网", youGo: "你要去", eventPage: "活动页面",
  accepted: "已收到", sentTitle: "活动已提交审核。", sentBody: "管理员处理后你会收到通知，通过的活动会出现在推荐流中。", understood: "知道了", submissionEyebrow: "社区活动", submissionTitle: "让全城看到你的创意。", name: "名称", nameHint: "独立电影之夜", dateTime: "日期和时间", category: "类别", description: "活动内容", descriptionHint: "请提供足够的信息，方便审核和参与者了解。", venueName: "场地", venueHint: "场地名称", address: "地址", addressHint: "街道和门牌号", tags: "标签（逗号分隔）", tagsHint: "电影, 聚会, 夜晚", price: "价格", priceHint: "500 ₽ 起", freeEntry: "免费入场", sending: "发送中…", sendModeration: "提交审核",
  loginTitle: "登录 Vayobyzh", loginBody: "选择常用平台。密码仍由该平台保管，你的滑动和计划会保留在 Vayobyzh。", guestMode: "当前为访客模式", providerUnavailable: "服务器尚未配置此登录平台", loginError: "无法开始登录，请重试。", logout: "退出登录", udmurtBeta: "乌德穆尔特语版本仍为测试版，欢迎帮助完善。", music: "音乐", exhibitions: "展览", theatre: "戏剧", cinema: "电影", lectures: "讲座", festivals: "节日", kids: "亲子", sport: "运动", other: "其他",
};

const udm: Copy = {
  brandHome: "Ваёбыж — азьло", kytchi: "Кытчи", plans: "Планъёс", friends: "Эшъёс", talks: "Вераськонъёс", ownEvent: "ас луон", signIn: "пыраны", account: "профиль", language: "Кыл",
  heroSwipe: "сертты", heroBoredom: "шугъяськон.", heroKicker: "Кытчи / аслыд люкам луонъёс", heroLead: "Афиша гинэ ӧвӧл. Улян радар: туннэ укшое мар луоз?", city: "Кар", yourCity: "Тынад каред",
  kytchiEyebrow: "01 / КЫТЧИ", swipeTitle: "Киен бырйиськод. Кытчи валанзэ тодэ вае.", swipeBody: "Паллян — уг яра. Бурлан яке тап — валэктон. «Мынӥсько» сямен луонъёсты тросмытэ но тодэ ваёнэз пыртэ.", tune: "интересъёсты тупатыны", demo: "демо-каталог · API ачиз пыроз", loading: "Кытчи лентадэ люка", notMine: "УГ ЯРА", look: "УЧКИСЬКО", details: "валэктон", dislike: "Уг яра", going: "Мынӥсько", feedDone: "Лента быдӥз. Умой палэн.", feedLearned: "Кытчи сертонъёстэ тодэ ваиз.", again: "Одигпол учкыны", signals: "ТЫНАД СИГНАЛЪЁСЫД", whatInterests: "Туннэ мар кызыктэ?", rebuild: "лентэз выльдыны",
  plansEyebrow: "02 / ПЛАНЪЁС", plansTitle: "Мыныны — со нин план.", plansBody: "Луон азьло одӥг нуналлы «Ваёбыж» приложениын тодэ ваёз. Берло Telegram но push пыртӥськом.", plansEmpty: "Карточка вылын сюлэмзэ басьты — берен план отын потоз.", peopleEyebrow: "03 / АДЯМИОС", peopleTitle: "Кин кытчы мынэ — тодӥськы.", peopleBody: "Эшъёс палэн планъёстэ гинэ адӟо. Соослэн быръемзы Кытчилы пумиськон луонъёсты шедьтыны юрттэ.", newFriend: "Выль эш", wantsFriend: "тонэ эшъёс пӧлы пыртыны малпа", requestSent: "курон келямын", noPlans: "планэз ӧй на", accepts: "басьтыны", addFriendTitle: "Эш ватсаны", addFriendBody: "Профилезлэн вакчи кодэз гожты. Телефон книга кулэ ӧвӧл.", friendExample: "шуом, katya", friendCode: "Эшлэн кодэз", yourCode: "Тынад кодэд",
  talksEyebrow: "04 / ВЕРАСЬКОНЪЁС", talksTitle: "Вераськон интые герӟаськемын.", town: "КАР", venue: "ИНТЫ", event: "ЛУОН", messages: "гожтэт", enter: "пыраны", communityEyebrow: "АДЯМИОСЛЭН ЛУОНЪЁСЫЗ", communityTitle: "Зэмос луон лэсьтӥды-а?", communityBody: "Луонэз ватсалэ. Администратор эскерем бере со лента но аслаз адямиослы рекомендацие пыроз.", propose: "луонэз ватсаны", footerLine: "Луонъёс тонэ шедьто. Кытчи — быръён понна.", close: "Петыны", kytchiSuggests: "КЫТЧИ СЁТЭ", whyYou: "Малы тонлы", when: "ку", where: "кытын", admission: "пырыськон", free: "Коньдон пумытэк", sitePrice: "Донэз сайт вылын", youGo: "Тон мынӥськод", eventPage: "луонлэн бамез",
  accepted: "БАСЬТИМ", sentTitle: "Луон эскерон вылэ келямын.", sentBody: "Администратор быръем бере тодэ ваён лыктоз, ӧдъяськем луон лентын потоз.", understood: "валай", submissionEyebrow: "ТОДОСЛЭН ЛУОНЭЗ", submissionTitle: "Карлы тодматэ, мар лэсьтӥды.", name: "Ним", nameHint: "Ас палэн кино уй", dateTime: "Нунал но вакыт", category: "Ужпум", description: "Мар луоз", descriptionHint: "Администратор но куноос валаны быгатозы сямен гожты.", venueName: "Инты", venueHint: "Интылэн нимыз", address: "Адрес", addressHint: "Урам, корка", tags: "Тегъёс запятой пыр", tagsHint: "кино, пумиськон, укшо", price: "Дон", priceHint: "500 ₽-ысь", freeEntry: "Коньдон пумытэк пырыны", sending: "келям…", sendModeration: "эскерон вылэ келяны",
  loginTitle: "Ваёбыж пырыны", loginBody: "Тодмо сервисэз бырйы. Пароль отын кылиз, сертонъёсыд но планъёсыд Ваёбыжын возьмиськозы.", guestMode: "Тон куно сямен учкиськод", providerUnavailable: "Та сервис серверын ӧй на тупаты", loginError: "Пырыськонэз кутыны ӧй быгаты. Таӵе одӥгпол.", logout: "потыны", udmurtBeta: "Удмурт кылъя версия — бета; кылъёсты тупатыны юрттэлэ.", music: "Крезьгур", exhibitions: "Выставкаос", theatre: "Театр", cinema: "Кино", lectures: "Лекциос", festivals: "Фестивальёс", kids: "Пиналъёсын", sport: "Спорт", other: "Мукет",
};

export const TRANSLATIONS: Record<Language, Copy> = { ru, en, zh, udm };

export const INTERESTS: { value: string; key: keyof Copy }[] = [
  { value: "Музыка", key: "music" }, { value: "Выставки", key: "exhibitions" },
  { value: "Театр", key: "theatre" }, { value: "Кино", key: "cinema" },
  { value: "Лекции", key: "lectures" }, { value: "Фестивали", key: "festivals" },
  { value: "С детьми", key: "kids" }, { value: "Спорт", key: "sport" },
];
