import {
  ArrowRight,
  Bell,
  Bot,
  BrainCircuit,
  CalendarDays,
  Check,
  ChevronRight,
  Clock3,
  ExternalLink,
  Heart,
  Languages,
  LogIn,
  MapPin,
  MessageCircle,
  Plus,
  Radio,
  RotateCcw,
  Send,
  SlidersHorizontal,
  Sparkles,
  Ticket,
  UserPlus,
  Users,
  X,
} from "lucide-react";
import { useEffect, useMemo, useRef, useState, type CSSProperties, type FormEvent } from "react";
import {
  addFriend,
  bootstrapSession,
  fetchAuthProviders,
  fetchCities,
  fetchDiscussions,
  fetchEvents,
  fetchFriends,
  fetchNotifications,
  fetchPlans,
  fetchProfile,
  fetchRecommendations,
  recordInteraction,
  saveInterests,
  setPlan,
  signOut,
  startOAuth,
  submitEvent,
} from "./api";
import { createDemoEvents, demoCities } from "./demo-data";
import { INTERESTS, LANGUAGES, LOCALES, TRANSLATIONS, type Language } from "./i18n";
import type {
  City,
  DateFilter,
  DiscussionSpace,
  EventItem,
  EventSubmission,
  Friend,
  NotificationItem,
  OAuthProvider,
  Profile,
  RecommendationFeed,
} from "./types";

const TONES = ["lime", "violet", "cyan", "coral", "amber", "blue"];

const AI_LABELS: Record<Language, {
  live: string;
  neuralFeed: string;
  match: string;
  confidence: string;
  signals: string;
  profile: string;
  why: string;
  next: string;
  learning: string;
  dismiss: string;
  open: string;
  going: string;
  tune: string;
  stage: Record<RecommendationFeed["learning_stage"], string>;
}> = {
  ru: {
    live: "AI ONLINE", neuralFeed: "ПЕРСОНАЛЬНАЯ НЕЙРОЛЕНТА", match: "совпадение",
    confidence: "уверенность модели", signals: "сигналов", profile: "ваш контекст",
    why: "почему это здесь", next: "следом в ленте", learning: "Кытчи перестраивает маршрут",
    dismiss: "не моё", open: "открыть", going: "пойду", tune: "настроить сигнал",
    stage: { exploring: "исследую", learning: "обучаюсь", personalized: "персонально" },
  },
  en: {
    live: "AI ONLINE", neuralFeed: "PERSONAL NEURAL FEED", match: "match",
    confidence: "model confidence", signals: "signals", profile: "your context",
    why: "why it is here", next: "next in your feed", learning: "Kytchi is rebuilding your route",
    dismiss: "not mine", open: "open", going: "going", tune: "tune signal",
    stage: { exploring: "exploring", learning: "learning", personalized: "personalized" },
  },
  zh: {
    live: "AI 在线", neuralFeed: "个性化神经推荐", match: "匹配度",
    confidence: "模型置信度", signals: "信号", profile: "你的偏好", why: "推荐原因",
    next: "接下来", learning: "Kytchi 正在更新推荐", dismiss: "不喜欢", open: "打开",
    going: "想去", tune: "调整信号",
    stage: { exploring: "探索中", learning: "学习中", personalized: "个性化" },
  },
  udm: {
    live: "AI УЖА", neuralFeed: "АСПОННА НЕЙРОЛЕНТА", match: "луонлык",
    confidence: "модельлэн чидатонэз", signals: "сигнал", profile: "тынад контекстэд",
    why: "малы та", next: "азьлань", learning: "Кытчи сюресэз выльдытэ",
    dismiss: "уг яра", open: "учкыны", going: "мынӥсько", tune: "сигналэз тупатыны",
    stage: { exploring: "утчасько", learning: "дышетскисько", personalized: "аслыд" },
  },
};

const DEMO_META: Omit<RecommendationFeed, "items"> = {
  strategy: "kytchi-ai-v2",
  learning_stage: "learning",
  signal_count: 7,
  confidence: 0.68,
  profile_summary: ["музыка", "искусство", "вечерний ритм"],
};

function readJson<T>(key: string, fallback: T): T {
  try {
    const value = localStorage.getItem(key);
    return value ? JSON.parse(value) as T : fallback;
  } catch {
    return fallback;
  }
}

function getUserId() {
  const stored = localStorage.getItem("vayobyzh-user-id");
  if (stored) return stored;
  const created = crypto.randomUUID();
  localStorage.setItem("vayobyzh-user-id", created);
  return created;
}

export function matchesDateFilter(isoDate: string, filter: DateFilter, now = new Date()) {
  if (filter === "any") return true;
  const date = new Date(isoDate);
  if (filter === "today") return date.toDateString() === now.toDateString();
  const diffDays = (date.getTime() - now.getTime()) / 86_400_000;
  if (filter === "week") return diffDays >= 0 && diffDays <= 7;
  return diffDays >= 0 && diffDays <= 7 && (date.getDay() === 0 || date.getDay() === 6);
}

function formatDate(value: string, language: Language) {
  return new Intl.DateTimeFormat(LOCALES[language], {
    weekday: "short",
    day: "numeric",
    month: "short",
    hour: "2-digit",
    minute: "2-digit",
  }).format(new Date(value)).replace(",", " ·");
}

function demoFriends(events: EventItem[]): Friend[] {
  return [
    { id: 1, status: "accepted", direction: "accepted", friend: { id: "katya", display_name: "Катя", city_slug: "msk", avatar_url: "" }, shared_events: events[1] ? [events[1]] : [] },
    { id: 2, status: "accepted", direction: "accepted", friend: { id: "misha", display_name: "Миша", city_slug: "msk", avatar_url: "" }, shared_events: events[0] ? [events[0]] : [] },
    { id: 3, status: "pending", direction: "incoming", friend: { id: "lena", display_name: "Лена", city_slug: "msk", avatar_url: "" }, shared_events: [] },
  ];
}

const DEMO_DISCUSSIONS: DiscussionSpace[] = [
  { id: 1, kind: "city", title: "Что происходит сегодня?", city_slug: "msk", event_id: null, venue_name: "", posts_count: 48 },
  { id: 2, kind: "venue", title: "Хлебозавод и вокруг", city_slug: "msk", event_id: null, venue_name: "Хлебозавод №9", posts_count: 19 },
  { id: 3, kind: "event", title: "Ищу компанию на выходные", city_slug: "msk", event_id: null, venue_name: "", posts_count: 27 },
];

function App() {
  const initialUserId = useMemo(getUserId, []);
  const [userId, setUserId] = useState(initialUserId);
  const [language, setLanguage] = useState<Language>(() => (localStorage.getItem("vayobyzh-language") as Language) || "ru");
  const [profile, setProfile] = useState<Profile | null>(null);
  const [providers, setProviders] = useState<OAuthProvider[]>([
    { id: "google", name: "Google", available: false },
    { id: "yandex", name: "Яндекс ID", available: false },
    { id: "github", name: "GitHub", available: false },
  ]);
  const [authOpen, setAuthOpen] = useState(false);
  const [authError, setAuthError] = useState(false);
  const [cities, setCities] = useState<City[]>(demoCities);
  const [city, setCity] = useState("msk");
  const [events, setEvents] = useState<EventItem[]>(createDemoEvents);
  const [initialEvents, setInitialEvents] = useState<EventItem[]>(createDemoEvents);
  const [feedMeta, setFeedMeta] = useState(DEMO_META);
  const [loading, setLoading] = useState(true);
  const [learning, setLearning] = useState(false);
  const [demoMode, setDemoMode] = useState(false);
  const [selected, setSelected] = useState<EventItem | null>(null);
  const [interests, setInterests] = useState<string[]>(() => readJson("vayobyzh-interests", ["Музыка", "Выставки"]));
  const [interestOpen, setInterestOpen] = useState(false);
  const [plannedIds, setPlannedIds] = useState<number[]>(() => readJson("vayobyzh-plans", []));
  const [plannedItems, setPlannedItems] = useState<EventItem[]>([]);
  const [friends, setFriends] = useState<Friend[]>([]);
  const [discussions, setDiscussions] = useState<DiscussionSpace[]>(DEMO_DISCUSSIONS);
  const [notifications, setNotifications] = useState<NotificationItem[]>([]);
  const [submitOpen, setSubmitOpen] = useState(false);
  const [submitState, setSubmitState] = useState<"idle" | "sending" | "done">("idle");
  const [friendCode, setFriendCode] = useState("");
  const [dragX, setDragX] = useState(0);
  const [dragging, setDragging] = useState(false);
  const pointerStart = useRef(0);
  const copy = TRANSLATIONS[language];
  const ai = AI_LABELS[language];
  const authenticated = Boolean(localStorage.getItem("vayobyzh-auth-provider"));

  useEffect(() => {
    document.documentElement.lang = language;
    localStorage.setItem("vayobyzh-language", language);
  }, [language]);

  useEffect(() => {
    let active = true;
    bootstrapSession()
      .then((id) => {
        if (!active) return;
        setUserId(id);
        return Promise.allSettled([fetchProfile(id), fetchAuthProviders()]);
      })
      .then((results) => {
        if (!active || !results) return;
        if (results[0].status === "fulfilled") setProfile(results[0].value);
        if (results[1].status === "fulfilled") setProviders(results[1].value);
      })
      .catch(() => undefined);
    return () => { active = false; };
  }, []);

  useEffect(() => {
    let active = true;
    setLoading(true);
    Promise.all([fetchCities(), fetchRecommendations(userId, city)])
      .then(([cityItems, feed]) => {
        if (!active) return;
        if (cityItems.length) setCities(cityItems);
        setEvents(feed.items);
        setInitialEvents(feed.items);
        setFeedMeta(feed);
        setDemoMode(false);
      })
      .catch(async () => {
        if (!active) return;
        try {
          const catalog = await fetchEvents(city);
          setEvents(catalog);
          setInitialEvents(catalog);
        } catch {
          const fallback = createDemoEvents().map((event) => ({ ...event, city_slug: city }));
          setEvents(fallback);
          setInitialEvents(fallback);
          setCities(demoCities);
          setDemoMode(true);
        }
      })
      .finally(() => active && setLoading(false));
    return () => { active = false; };
  }, [city, userId]);

  useEffect(() => {
    Promise.allSettled([
      fetchPlans(userId),
      fetchFriends(userId),
      fetchNotifications(userId),
      fetchDiscussions(city),
    ]).then(([plansResult, friendsResult, notificationsResult, discussionsResult]) => {
      if (plansResult.status === "fulfilled") {
        setPlannedIds(plansResult.value.map((item) => item.event.id));
        setPlannedItems(plansResult.value.map((item) => item.event));
      }
      if (friendsResult.status === "fulfilled" && friendsResult.value.length) setFriends(friendsResult.value);
      else setFriends(demoFriends(initialEvents));
      if (notificationsResult.status === "fulfilled") setNotifications(notificationsResult.value);
      if (discussionsResult.status === "fulfilled" && discussionsResult.value.length) setDiscussions(discussionsResult.value);
    });
  }, [city, initialEvents, userId]);

  useEffect(() => localStorage.setItem("vayobyzh-plans", JSON.stringify(plannedIds)), [plannedIds]);

  useEffect(() => {
    if (!selected && !submitOpen && !authOpen) return;
    document.body.classList.add("modal-open");
    const close = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setSelected(null);
        setSubmitOpen(false);
        setAuthOpen(false);
      }
    };
    window.addEventListener("keydown", close);
    return () => {
      document.body.classList.remove("modal-open");
      window.removeEventListener("keydown", close);
    };
  }, [selected, submitOpen, authOpen]);

  const current = events[0];
  const upcoming = events.slice(1, 4);
  const cityName = cities.find((item) => item.slug === city)?.name ?? copy.yourCity;
  const plannedEvents = [...new Map([
    ...plannedItems,
    ...initialEvents.filter((event) => plannedIds.includes(event.id)),
  ].map((event) => [event.id, event])).values()];
  const matchPercent = current?.match_percent ?? Math.round((current?.score ?? 0.68) * 100);

  const refreshFeed = async () => {
    setLearning(true);
    try {
      const feed = await fetchRecommendations(userId, city);
      setEvents(feed.items);
      setInitialEvents((items) => {
        const byId = new Map([...items, ...feed.items].map((item) => [item.id, item]));
        return [...byId.values()];
      });
      setFeedMeta(feed);
    } finally {
      window.setTimeout(() => setLearning(false), 420);
    }
  };

  const dismissCurrent = () => {
    if (!current) return;
    setEvents((items) => items.filter((item) => item.id !== current.id));
    setDragX(0);
    setLearning(true);
    void recordInteraction(userId, current.id, "dismiss")
      .then(refreshFeed)
      .catch(() => setLearning(false));
  };

  const showDetails = (event: EventItem | undefined = current) => {
    if (!event) return;
    setSelected(event);
    setDragX(0);
    void recordInteraction(userId, event.id, "open").catch(() => undefined);
  };

  const markGoing = (event: EventItem) => {
    setPlannedIds((ids) => ids.includes(event.id) ? ids : [...ids, event.id]);
    setPlannedItems((items) => items.some((item) => item.id === event.id) ? items : [...items, event]);
    setLearning(true);
    void setPlan(userId, event.id)
      .then(refreshFeed)
      .catch(() => setLearning(false));
  };

  const applyInterests = () => {
    localStorage.setItem("vayobyzh-interests", JSON.stringify(interests));
    setInterestOpen(false);
    setLearning(true);
    void saveInterests(userId, interests)
      .then(refreshFeed)
      .catch(() => setLearning(false));
  };

  const handleFriend = async () => {
    const code = friendCode.trim();
    if (!code) return;
    try { await addFriend(userId, code); } catch { /* Demo mode keeps the optimistic row. */ }
    setFriends((items) => [...items, {
      id: Date.now(), status: "pending", direction: "outgoing",
      friend: { id: code, display_name: code, city_slug: city, avatar_url: "" }, shared_events: [],
    }]);
    setFriendCode("");
  };

  const handleSubmission = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const data = new FormData(event.currentTarget);
    const payload: EventSubmission = {
      city_slug: city,
      title: String(data.get("title") ?? ""),
      description: String(data.get("description") ?? ""),
      category: String(data.get("category") ?? "Другое"),
      tags: String(data.get("tags") ?? "").split(",").map((tag) => tag.trim()).filter(Boolean),
      starts_at: new Date(String(data.get("starts_at") ?? "")).toISOString(),
      venue_name: String(data.get("venue_name") ?? ""),
      address: String(data.get("address") ?? ""),
      price_text: String(data.get("price_text") ?? ""),
      is_free: data.get("is_free") === "on",
    };
    setSubmitState("sending");
    await submitEvent(userId, payload).catch(() => undefined);
    setSubmitState("done");
  };

  const handleOAuth = async (provider: OAuthProvider) => {
    if (!provider.available) return;
    setAuthError(false);
    try { await startOAuth(provider.id, userId); } catch { setAuthError(true); }
  };

  const ringStyle = { "--progress": `${Math.round(feedMeta.confidence * 100) * 3.6}deg` } as CSSProperties;

  return (
    <div className="app-shell" id="top">
      <header className="site-header">
        <a className="brand" href="#top" aria-label={copy.brandHome}>
          <span className="brand-sigil"><i /><i /><i /></span>
          <span className="brand-name">ваёбыж</span>
          <b>1.1</b>
        </a>
        <nav aria-label={copy.talks}>
          <a href="#kytchi">01 {copy.kytchi}</a>
          <a href="#plans">02 {copy.plans}</a>
          <a href="#people">03 {copy.friends}</a>
          <a href="#talks">04 {copy.talks}</a>
        </nav>
        <div className="header-tools">
          <label className="language-picker" title={copy.language}>
            <Languages size={16} />
            <span>{LANGUAGES.find((item) => item.id === language)?.short}</span>
            <select value={language} onChange={(event) => setLanguage(event.target.value as Language)} aria-label={copy.language}>
              {LANGUAGES.map((item) => <option key={item.id} value={item.id}>{item.label}</option>)}
            </select>
          </label>
          <button className="account-button" type="button" onClick={() => setAuthOpen(true)}>
            <LogIn size={16} /><span>{authenticated ? profile?.display_name || copy.account : copy.signIn}</span>
          </button>
        </div>
      </header>

      <main>
        <section className="hero" id="kytchi">
          <div className="hero-grid" aria-hidden="true" />
          <div className="hero-copy">
            <div className="live-chip"><Radio size={13} /> {ai.live} <span>{ai.neuralFeed}</span></div>
            <h1><span>{copy.heroSwipe}</span><strong>{copy.heroBoredom}</strong></h1>
            <p>{copy.heroLead}</p>
            <div className="hero-controls">
              <label className="city-picker">
                <MapPin size={17} /><span>{cityName}</span>
                <select value={city} onChange={(event) => setCity(event.target.value)} aria-label={copy.city}>
                  {cities.map((item) => <option key={item.slug} value={item.slug}>{item.name}</option>)}
                </select>
              </label>
              <button type="button" onClick={() => setInterestOpen((value) => !value)}>
                <SlidersHorizontal size={17} /> {ai.tune}
              </button>
            </div>
          </div>
          <div className="hero-orbit" aria-hidden="true">
            <div className="orbit-core"><BrainCircuit /></div>
            <span className="orbit-one" /><span className="orbit-two" /><span className="orbit-three" />
            <b>KYTCHI<br />CORE</b>
          </div>
          <div className="hero-index" aria-hidden="true">∞</div>
        </section>

        <section className="recommendation-lab" aria-labelledby="feed-title">
          <div className="lab-head">
            <div><span>01 / KYTCHI AI</span><h2 id="feed-title">{copy.swipeTitle}</h2></div>
            <p>{copy.swipeBody}</p>
          </div>

          {interestOpen && (
            <div className="signal-panel">
              <div><span>{copy.signals}</span><h3>{copy.whatInterests}</h3></div>
              <div className="interest-chips">
                {INTERESTS.map((item) => (
                  <button
                    key={item.value}
                    type="button"
                    className={interests.includes(item.value) ? "selected" : ""}
                    onClick={() => setInterests((values) => values.includes(item.value)
                      ? values.filter((value) => value !== item.value)
                      : [...values, item.value])}
                  >
                    {interests.includes(item.value) && <Check size={14} />}{copy[item.key]}
                  </button>
                ))}
              </div>
              <button className="apply-signals" type="button" onClick={applyInterests}>
                {copy.rebuild}<ArrowRight size={17} />
              </button>
            </div>
          )}

          <div className="lab-grid">
            <aside className="ai-console">
              <div className="console-top">
                <div className="confidence-ring" style={ringStyle}>
                  <span>{Math.round(feedMeta.confidence * 100)}</span><small>%</small>
                </div>
                <div><span>{ai.confidence}</span><strong>{ai.stage[feedMeta.learning_stage]}</strong></div>
              </div>
              <div className="console-stat"><span>{String(feedMeta.signal_count).padStart(2, "0")}</span><p>{ai.signals}<small>{feedMeta.strategy}</small></p></div>
              <div className="context-list">
                <span>{ai.profile}</span>
                <div>{feedMeta.profile_summary.length
                  ? feedMeta.profile_summary.map((item) => <b key={item}>#{item.replaceAll(" ", "_")}</b>)
                  : <b>#новый_профиль</b>}
                </div>
              </div>
              <div className="reason-stack">
                <span>{ai.why}</span>
                {(current?.reasons ?? [copy.heroKicker]).slice(0, 3).map((reason, index) => (
                  <p key={reason}><i>{String(index + 1).padStart(2, "0")}</i>{reason}</p>
                ))}
              </div>
              {demoMode && <div className="demo-note"><Sparkles size={14} /> {copy.demo}</div>}
            </aside>

            <div className={`event-deck ${learning ? "is-learning" : ""}`}>
              {learning && <div className="learning-overlay"><BrainCircuit /><span>{ai.learning}</span></div>}
              {loading ? (
                <div className="feed-loading"><span /><p>{copy.loading}</p></div>
              ) : current ? (
                <>
                  <article
                    className={`event-card tone-${TONES[current.id % TONES.length]} ${dragging ? "is-dragging" : ""}`}
                    style={{ transform: `translateX(${dragX}px) rotate(${dragX / 34}deg)` }}
                    onPointerDown={(event) => {
                      pointerStart.current = event.clientX;
                      setDragging(true);
                      event.currentTarget.setPointerCapture(event.pointerId);
                    }}
                    onPointerMove={(event) => dragging && setDragX(event.clientX - pointerStart.current)}
                    onPointerUp={() => {
                      setDragging(false);
                      if (dragX < -100) dismissCurrent();
                      else if (dragX > 100) showDetails();
                      else setDragX(0);
                    }}
                    onPointerCancel={() => { setDragging(false); setDragX(0); }}
                  >
                    <div className="event-visual">
                      {current.image_url
                        ? <img src={current.image_url} alt="" draggable="false" />
                        : <div className="generated-visual"><span>{current.category.slice(0, 1)}</span><i /><b /></div>}
                      <div className="match-badge"><strong>{matchPercent}%</strong><span>{ai.match}</span></div>
                      <div className="event-sequence">REC · {String(initialEvents.length - events.length + 1).padStart(2, "0")}</div>
                      {dragX < -35 && <strong className="gesture-label gesture-no">{ai.dismiss}</strong>}
                      {dragX > 35 && <strong className="gesture-label gesture-yes">{ai.open}</strong>}
                    </div>
                    <div className="event-copy">
                      <div className="event-meta"><span>{current.category}</span><span>{formatDate(current.starts_at, language)}</span></div>
                      <h3>{current.title}</h3>
                      <p><MapPin size={15} /> {current.venue_name || current.address}</p>
                      <div className="event-tags">{current.tags.slice(0, 3).map((tag) => <span key={tag}>{tag}</span>)}</div>
                    </div>
                  </article>
                  <div className="deck-actions">
                    <button className="action-dismiss" type="button" onClick={dismissCurrent}><X /><span>{ai.dismiss}</span></button>
                    <button className="action-open" type="button" onClick={() => showDetails()}><span>{ai.open}</span><ArrowRight /></button>
                    <button className={`action-going ${plannedIds.includes(current.id) ? "selected" : ""}`} type="button" onClick={() => markGoing(current)}>
                      <Heart fill={plannedIds.includes(current.id) ? "currentColor" : "none"} /><span>{ai.going}</span>
                    </button>
                  </div>
                </>
              ) : (
                <div className="feed-empty"><RotateCcw /><h3>{copy.feedDone}</h3><p>{copy.feedLearned}</p><button type="button" onClick={() => setEvents(initialEvents)}>{copy.again}</button></div>
              )}
            </div>
          </div>

          <div className="up-next">
            <div className="up-next-label"><span>{ai.next}</span><i>{upcoming.length}</i></div>
            <div className="up-next-track">
              {upcoming.map((event, index) => (
                <button type="button" className="next-event" key={event.id} onClick={() => showDetails(event)}>
                  <span>{String(index + 2).padStart(2, "0")}</span>
                  <div><small>{event.category} · {event.match_percent ?? Math.round((event.score ?? .6) * 100)}%</small><strong>{event.title}</strong></div>
                  <ChevronRight />
                </button>
              ))}
            </div>
          </div>
        </section>

        <section className="plans-section" id="plans">
          <div className="section-kicker">02 / {copy.plans}</div>
          <div className="section-heading"><h2>{copy.plansTitle}</h2><p>{copy.plansBody}</p></div>
          {notifications.filter((item) => !item.read_at).slice(0, 1).map((item) => (
            <div className="notification" key={item.id}><Bell /><strong>{item.title}</strong><span>{item.body}</span></div>
          ))}
          <div className="plans-grid">
            {plannedEvents.length ? plannedEvents.map((event, index) => (
              <button className="plan-card" type="button" key={event.id} onClick={() => showDetails(event)}>
                <span>PLAN · {String(index + 1).padStart(2, "0")}</span>
                <strong>{event.title}</strong>
                <small>{formatDate(event.starts_at, language)} · {event.venue_name}</small>
                <ArrowRight />
              </button>
            )) : (
              <div className="plan-empty"><Heart /><p>{copy.plansEmpty}</p></div>
            )}
          </div>
        </section>

        <section className="people-section" id="people">
          <div className="people-copy">
            <div className="section-kicker">03 / {copy.friends}</div>
            <h2>{copy.peopleTitle}</h2><p>{copy.peopleBody}</p>
            <div className="add-friend">
              <UserPlus />
              <input value={friendCode} onChange={(event) => setFriendCode(event.target.value)} placeholder={copy.friendExample} aria-label={copy.friendCode} />
              <button type="button" onClick={handleFriend}><Send /></button>
            </div>
            <small>{copy.yourCode}: {userId.slice(0, 8)}</small>
          </div>
          <div className="friend-stack">
            {friends.slice(0, 5).map((item, index) => (
              <article className="friend-row" key={item.id}>
                <span className={`avatar tone-${TONES[index % TONES.length]}`}>{item.friend.display_name.slice(0, 1).toUpperCase()}</span>
                <div><strong>{item.friend.display_name || copy.newFriend}</strong><p>{item.direction === "incoming" ? copy.wantsFriend : item.status === "pending" ? copy.requestSent : item.shared_events[0] ? `${copy.going}: ${item.shared_events[0].title}` : copy.noPlans}</p></div>
                {item.direction === "incoming" ? <button type="button">{copy.accepts}</button> : <Users />}
              </article>
            ))}
          </div>
        </section>

        <section className="talks-section" id="talks">
          <div className="section-kicker">04 / {copy.talks}</div>
          <div className="section-heading"><h2>{copy.talksTitle}</h2><MessageCircle /></div>
          <div className="talk-grid">
            {discussions.slice(0, 3).map((space, index) => (
              <article key={space.id}>
                <span>0{index + 1} · {space.kind === "city" ? copy.town : space.kind === "venue" ? copy.venue : copy.event}</span>
                <h3>{space.title}</h3><p>{space.posts_count} {copy.messages}</p>
                <button type="button">{copy.enter}<ArrowRight /></button>
              </article>
            ))}
          </div>
        </section>

        <section className="community-cta">
          <div className="cta-orb" aria-hidden="true"><Plus /></div>
          <span>{copy.communityEyebrow}</span><h2>{copy.communityTitle}</h2><p>{copy.communityBody}</p>
          <button type="button" onClick={() => { setSubmitState("idle"); setSubmitOpen(true); }}>{copy.propose}<Plus /></button>
        </section>
      </main>

      <footer>
        <a className="brand" href="#top"><span className="brand-sigil"><i /><i /><i /></span><span className="brand-name">ваёбыж</span></a>
        <p>{copy.footerLine}</p>
        <div><a href="#talks">{copy.talks}</a><a href="mailto:hello@vayobyzh.ru">hello@vayobyzh.ru</a></div>
      </footer>

      {selected && (
        <div className="modal-backdrop" onMouseDown={(event) => event.target === event.currentTarget && setSelected(null)}>
          <article className="event-modal" role="dialog" aria-modal="true" aria-labelledby="event-title">
            <button className="modal-close" type="button" onClick={() => setSelected(null)} aria-label={copy.close}><X /></button>
            <div className={`modal-visual tone-${TONES[selected.id % TONES.length]}`}>
              {selected.image_url ? <img src={selected.image_url} alt="" /> : <span>{selected.category.slice(0, 1)}</span>}
              <div><strong>{selected.match_percent ?? Math.round((selected.score ?? .68) * 100)}%</strong><small>{ai.match}</small></div>
            </div>
            <div className="modal-copy">
              <span className="modal-kicker"><Bot /> {copy.kytchiSuggests}</span>
              <h2 id="event-title">{selected.title}</h2>
              <p className="modal-description">{selected.description}</p>
              <div className="modal-reasons"><span>{copy.whyYou}</span>{selected.reasons?.map((reason) => <p key={reason}><Sparkles />{reason}</p>)}</div>
              <dl>
                <div><dt><Clock3 />{copy.when}</dt><dd>{formatDate(selected.starts_at, language)}</dd></div>
                <div><dt><MapPin />{copy.where}</dt><dd>{selected.venue_name}<small>{selected.address}</small></dd></div>
                <div><dt><Ticket />{copy.admission}</dt><dd>{selected.is_free ? copy.free : selected.price_text || copy.sitePrice}</dd></div>
              </dl>
              <div className="modal-actions">
                <button className="modal-going" type="button" onClick={() => markGoing(selected)}><Heart />{plannedIds.includes(selected.id) ? copy.youGo : copy.going}</button>
                <a href={selected.event_url} target="_blank" rel="noreferrer">{copy.eventPage}<ExternalLink /></a>
              </div>
            </div>
          </article>
        </div>
      )}

      {submitOpen && (
        <div className="modal-backdrop" onMouseDown={(event) => event.target === event.currentTarget && setSubmitOpen(false)}>
          <section className="form-modal" role="dialog" aria-modal="true" aria-labelledby="submit-title">
            <button className="modal-close" type="button" onClick={() => setSubmitOpen(false)} aria-label={copy.close}><X /></button>
            {submitState === "done" ? (
              <div className="submit-done"><Check /><span>{copy.accepted}</span><h2>{copy.sentTitle}</h2><p>{copy.sentBody}</p><button type="button" onClick={() => setSubmitOpen(false)}>{copy.understood}</button></div>
            ) : (
              <form onSubmit={handleSubmission}>
                <span>{copy.submissionEyebrow}</span><h2 id="submit-title">{copy.submissionTitle}</h2>
                <div className="form-grid">
                  <label>{copy.name}<input required minLength={4} name="title" placeholder={copy.nameHint} /></label>
                  <label>{copy.dateTime}<input required name="starts_at" type="datetime-local" /></label>
                  <label>{copy.category}<select name="category">{INTERESTS.map((item) => <option key={item.value}>{copy[item.key]}</option>)}</select></label>
                  <label>{copy.venueName}<input required name="venue_name" placeholder={copy.venueHint} /></label>
                  <label className="wide">{copy.description}<textarea required minLength={20} name="description" placeholder={copy.descriptionHint} /></label>
                  <label>{copy.address}<input required name="address" placeholder={copy.addressHint} /></label>
                  <label>{copy.tags}<input name="tags" placeholder={copy.tagsHint} /></label>
                  <label>{copy.price}<input name="price_text" placeholder={copy.priceHint} /></label>
                  <label className="checkbox"><input type="checkbox" name="is_free" />{copy.freeEntry}</label>
                </div>
                <button className="send-form" type="submit" disabled={submitState === "sending"}>{submitState === "sending" ? copy.sending : copy.sendModeration}<Send /></button>
              </form>
            )}
          </section>
        </div>
      )}

      {authOpen && (
        <div className="modal-backdrop" onMouseDown={(event) => event.target === event.currentTarget && setAuthOpen(false)}>
          <section className="auth-modal" role="dialog" aria-modal="true" aria-labelledby="auth-title">
            <button className="modal-close" type="button" onClick={() => setAuthOpen(false)} aria-label={copy.close}><X /></button>
            <span>IDENTITY / 01</span><h2 id="auth-title">{copy.loginTitle}</h2><p>{copy.loginBody}</p>
            {profile?.avatar_url && <img className="profile-avatar" src={profile.avatar_url} alt="" />}
            <div className="provider-list">
              {providers.map((provider) => (
                <button key={provider.id} type="button" disabled={!provider.available} onClick={() => handleOAuth(provider)}>
                  <span>{provider.name.slice(0, 1)}</span><strong>{provider.name}</strong><ChevronRight />
                  {!provider.available && <small>{copy.providerUnavailable}</small>}
                </button>
              ))}
            </div>
            {authError && <p className="auth-error">{copy.loginError}</p>}
            {authenticated && <button className="logout-button" type="button" onClick={signOut}>{copy.logout}</button>}
          </section>
        </div>
      )}
    </div>
  );
}

export default App;
