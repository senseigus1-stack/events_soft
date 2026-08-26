import {
  ArrowRight, Bell, CalendarDays, Check, ChevronRight, Clock3, ExternalLink,
  Heart, MapPin, MessageCircle, Plus, RotateCcw, Send, Sparkles, Ticket,
  UserPlus, Users, X, Zap,
} from "lucide-react";
import { useEffect, useMemo, useRef, useState } from "react";
import {
  addFriend, fetchCities, fetchDiscussions, fetchEvents, fetchFriends,
  fetchNotifications, fetchPlans, fetchRecommendations, recordInteraction,
  saveInterests, setPlan, submitEvent,
} from "./api";
import { createDemoEvents, demoCities } from "./demo-data";
import type {
  City, DateFilter, DiscussionSpace, EventItem, EventSubmission, Friend,
  NotificationItem,
} from "./types";

const INTERESTS = ["Музыка", "Выставки", "Театр", "Кино", "Лекции", "Фестивали", "С детьми", "Спорт"];
const TONES = ["acid", "orange", "blue", "pink", "violet", "aqua"];

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

function formatDate(value: string) {
  return new Intl.DateTimeFormat("ru-RU", {
    weekday: "short", day: "numeric", month: "short", hour: "2-digit", minute: "2-digit",
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
  const userId = useMemo(getUserId, []);
  const [cities, setCities] = useState<City[]>(demoCities);
  const [city, setCity] = useState("msk");
  const [events, setEvents] = useState<EventItem[]>(createDemoEvents);
  const [initialEvents, setInitialEvents] = useState<EventItem[]>(createDemoEvents);
  const [loading, setLoading] = useState(true);
  const [demoMode, setDemoMode] = useState(false);
  const [selected, setSelected] = useState<EventItem | null>(null);
  const [interests, setInterests] = useState<string[]>(() => readJson("vayobyzh-interests", ["Музыка", "Выставки"]));
  const [interestOpen, setInterestOpen] = useState(false);
  const [plannedIds, setPlannedIds] = useState<number[]>(() => readJson("vayobyzh-plans", []));
  const [friends, setFriends] = useState<Friend[]>([]);
  const [discussions, setDiscussions] = useState<DiscussionSpace[]>(DEMO_DISCUSSIONS);
  const [notifications, setNotifications] = useState<NotificationItem[]>([]);
  const [submitOpen, setSubmitOpen] = useState(false);
  const [submitState, setSubmitState] = useState<"idle" | "sending" | "done">("idle");
  const [friendCode, setFriendCode] = useState("");
  const [dragX, setDragX] = useState(0);
  const [dragging, setDragging] = useState(false);
  const pointerStart = useRef(0);

  useEffect(() => {
    let active = true;
    setLoading(true);
    Promise.all([
      fetchCities(),
      fetchRecommendations(userId, city).catch(() => fetchEvents(city)),
    ]).then(([cityItems, eventItems]) => {
      if (!active) return;
      if (cityItems.length) setCities(cityItems);
      setEvents(eventItems);
      setInitialEvents(eventItems);
      setDemoMode(false);
    }).catch(() => {
      if (!active) return;
      const fallback = createDemoEvents().map((event) => ({ ...event, city_slug: city }));
      setEvents(fallback);
      setInitialEvents(fallback);
      setCities(demoCities);
      setDemoMode(true);
    }).finally(() => active && setLoading(false));
    return () => { active = false; };
  }, [city, userId]);

  useEffect(() => {
    Promise.allSettled([fetchPlans(userId), fetchFriends(userId), fetchNotifications(userId), fetchDiscussions(city)])
      .then(([plansResult, friendsResult, notificationsResult, discussionsResult]) => {
        if (plansResult.status === "fulfilled") setPlannedIds(plansResult.value.map((item) => item.event.id));
        if (friendsResult.status === "fulfilled" && friendsResult.value.length) setFriends(friendsResult.value);
        else setFriends(demoFriends(initialEvents));
        if (notificationsResult.status === "fulfilled") setNotifications(notificationsResult.value);
        if (discussionsResult.status === "fulfilled" && discussionsResult.value.length) setDiscussions(discussionsResult.value);
      });
  }, [city, initialEvents, userId]);

  useEffect(() => localStorage.setItem("vayobyzh-plans", JSON.stringify(plannedIds)), [plannedIds]);
  useEffect(() => {
    if (!selected && !submitOpen) return;
    document.body.classList.add("modal-open");
    const close = (event: KeyboardEvent) => {
      if (event.key === "Escape") { setSelected(null); setSubmitOpen(false); }
    };
    window.addEventListener("keydown", close);
    return () => { document.body.classList.remove("modal-open"); window.removeEventListener("keydown", close); };
  }, [selected, submitOpen]);

  const current = events[0];
  const next = events[1];
  const cityName = cities.find((item) => item.slug === city)?.name ?? "Ваш город";
  const plannedEvents = initialEvents.filter((event) => plannedIds.includes(event.id));

  const dismissCurrent = () => {
    if (!current) return;
    setEvents((items) => items.filter((item) => item.id !== current.id));
    setDragX(0);
    void recordInteraction(userId, current.id, "dismiss").catch(() => undefined);
  };

  const showDetails = (event: EventItem | undefined = current) => {
    if (!event) return;
    setSelected(event);
    setDragX(0);
    void recordInteraction(userId, event.id, "open").catch(() => undefined);
  };

  const markGoing = (event: EventItem) => {
    setPlannedIds((ids) => ids.includes(event.id) ? ids : [...ids, event.id]);
    void setPlan(userId, event.id).catch(() => undefined);
  };

  const applyInterests = () => {
    localStorage.setItem("vayobyzh-interests", JSON.stringify(interests));
    setInterestOpen(false);
    void saveInterests(userId, interests)
      .then(() => fetchRecommendations(userId, city))
      .then((items) => { setEvents(items); setInitialEvents(items); })
      .catch(() => undefined);
  };

  const handleFriend = async () => {
    const code = friendCode.trim();
    if (!code) return;
    try { await addFriend(userId, code); } catch { /* local preview keeps the optimistic card */ }
    setFriends((items) => [...items, { id: Date.now(), status: "pending", direction: "outgoing", friend: { id: code, display_name: code, city_slug: city, avatar_url: "" }, shared_events: [] }]);
    setFriendCode("");
  };

  const handleSubmission = async (event: React.FormEvent<HTMLFormElement>) => {
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

  return (
    <div className="app-shell" id="top">
      <header className="site-header">
        <a className="brand" href="#top" aria-label="Ваёбыж — на главную"><img src="/avatar.png" alt="" /><span>ваёбыж</span><i>beta</i></a>
        <nav aria-label="Навигация"><a href="#kytchi">Кытчи</a><a href="#plans">Планы</a><a href="#friends">Друзья</a><a href="#talks">Обсуждения</a></nav>
        <button className="submit-event-button" type="button" onClick={() => { setSubmitState("idle"); setSubmitOpen(true); }}><Plus size={18} /> своё событие</button>
      </header>

      <main>
        <section className="hero" id="kytchi">
          <div className="hero-word"><span>смахни</span><strong>скуку.</strong></div>
          <div className="hero-side"><p><Zap size={17} /> Кытчи / персональная подборка</p><h1>Не афиша. Живой радар того, что может стать вашим вечером.</h1><label className="city-picker"><MapPin size={18} /><span>{cityName}</span><select value={city} onChange={(event) => setCity(event.target.value)} aria-label="Город">{cities.map((item) => <option key={item.slug} value={item.slug}>{item.name}</option>)}</select></label></div>
        </section>

        <section className="swipe-zone" aria-labelledby="swipe-title">
          <aside className="swipe-aside"><p className="eyebrow">01 / КЫТЧИ</p><h2 id="swipe-title">Вы решаете жестом.<br />Кытчи запоминает смысл.</h2><p>Влево — не ваше. Вправо или тап — подробности. «Пойду» усиливает похожие рекомендации и включает напоминание.</p><button className="tune-button" type="button" onClick={() => setInterestOpen((value) => !value)}><Sparkles size={17} /> настроить интересы <ChevronRight size={17} /></button>{demoMode && <span className="demo-badge">демо-каталог · API подключается автоматически</span>}</aside>
          <div className="deck-wrap">
            {loading ? <div className="deck-loading"><span /><p>Кытчи собирает вашу ленту</p></div> : current ? <>
              {next && <article className={`swipe-card next-card tone-${TONES[next.id % TONES.length]}`} aria-hidden="true"><div /></article>}
              <article className={`swipe-card active-card tone-${TONES[current.id % TONES.length]} ${dragging ? "is-dragging" : ""}`} style={{ transform: `translateX(${dragX}px) rotate(${dragX / 24}deg)` }} onPointerDown={(event) => { pointerStart.current = event.clientX; setDragging(true); event.currentTarget.setPointerCapture(event.pointerId); }} onPointerMove={(event) => dragging && setDragX(event.clientX - pointerStart.current)} onPointerUp={() => { setDragging(false); if (dragX < -90) dismissCurrent(); else if (dragX > 90) showDetails(); else setDragX(0); }}>
                <div className="swipe-art">{current.image_url ? <img src={current.image_url} alt="" draggable="false" /> : <div className="generated-art"><span>{current.category.slice(0, 1)}</span><i /><b /></div>}<span className="card-counter">{String(initialEvents.length - events.length + 1).padStart(2, "0")} / {String(initialEvents.length).padStart(2, "0")}</span><span className="card-category">{current.category}</span>{dragX < -35 && <strong className="gesture-label no-label">НЕ МОЁ</strong>}{dragX > 35 && <strong className="gesture-label yes-label">СМОТРЮ</strong>}</div>
                <div className="swipe-copy" onClick={() => showDetails()}><p className="event-date"><CalendarDays size={16} /> {formatDate(current.starts_at)}</p><h3>{current.title}</h3><p className="event-place"><MapPin size={16} /> {current.venue_name}</p>{current.reasons?.[0] && <span className="kytchi-reason"><Sparkles size={15} /> {current.reasons[0]}</span>}</div>
              </article>
              <div className="swipe-actions"><button className="reject-action" type="button" onClick={dismissCurrent} aria-label="Не интересно"><X /></button><button className="details-action" type="button" onClick={() => showDetails()}><span>подробнее</span><ArrowRight /></button><button className={`going-action ${plannedIds.includes(current.id) ? "is-going" : ""}`} type="button" onClick={() => markGoing(current)} aria-label="Пойду"><Heart fill={plannedIds.includes(current.id) ? "currentColor" : "none"} /></button></div>
            </> : <div className="deck-empty"><RotateCcw size={34} /><h3>Лента закончилась.<br />Хороший знак.</h3><p>Кытчи уже понял ваши свайпы.</p><button type="button" onClick={() => setEvents(initialEvents)}>Посмотреть ещё раз</button></div>}
          </div>
        </section>

        {interestOpen && <section className="interest-ribbon"><div><p className="eyebrow">ВАШИ СИГНАЛЫ</p><h2>Что сейчас цепляет?</h2></div><div className="interest-chips">{INTERESTS.map((item) => <button key={item} type="button" className={interests.includes(item) ? "selected" : ""} onClick={() => setInterests((values) => values.includes(item) ? values.filter((value) => value !== item) : [...values, item])}>{interests.includes(item) && <Check size={16} />}{item}</button>)}</div><button className="save-taste" type="button" onClick={applyInterests}>пересобрать ленту <ArrowRight size={18} /></button></section>}

        <section className="plans-section" id="plans"><div className="section-title"><div><p className="eyebrow">02 / ПЛАНЫ</p><h2>Пойти — это уже план.</h2></div><p>За сутки до события «Ваёбыж» напомнит внутри приложения. Позже сюда легко подключаются Telegram и push.</p></div>{notifications.filter((item) => !item.read_at).slice(0, 1).map((item) => <div className="notification" key={item.id}><Bell size={19} /><strong>{item.title}</strong><span>{item.body}</span></div>)}<div className="plans-track">{plannedEvents.length ? plannedEvents.map((event) => <button className="plan-card" type="button" key={event.id} onClick={() => showDetails(event)}><span>{formatDate(event.starts_at)}</span><strong>{event.title}</strong><small>{event.venue_name}</small><ChevronRight /></button>) : <div className="plan-placeholder"><Heart /><p>Нажмите сердце на карточке — первый план появится здесь.</p></div>}</div></section>

        <section className="friends-section" id="friends"><div className="section-title"><div><p className="eyebrow">03 / ЛЮДИ</p><h2>Не гадать, кто куда.</h2></div><p>Друзья видят только отмеченные планы. Их выбор помогает Кытчи находить события, на которых вы реально встретитесь.</p></div><div className="friend-layout"><div className="friend-list">{friends.slice(0, 6).map((item, index) => <article className="friend-card" key={item.id}><span className={`friend-avatar friend-tone-${index % 4}`}>{item.friend.display_name.slice(0, 1).toUpperCase()}</span><div><strong>{item.friend.display_name || "Новый друг"}</strong><p>{item.direction === "incoming" ? "хочет добавить вас" : item.status === "pending" ? "запрос отправлен" : item.shared_events[0] ? `идёт: ${item.shared_events[0].title}` : "пока без планов"}</p></div>{item.direction === "incoming" ? <button type="button">принять</button> : <Users size={18} />}</article>)}</div><div className="add-friend-card"><UserPlus size={28} /><h3>Добавить своего</h3><p>Вставьте короткий код профиля. Без поиска по телефонной книге.</p><div><input value={friendCode} onChange={(event) => setFriendCode(event.target.value)} placeholder="например, katya" aria-label="Код друга" /><button type="button" onClick={handleFriend}><Send size={18} /></button></div><small>Ваш код: {userId.slice(0, 8)}</small></div></div></section>

        <section className="talks-section" id="talks"><div className="talks-head"><div><p className="eyebrow">04 / ОБСУЖДАЛКИ</p><h2>Разговор привязан к месту.</h2></div><MessageCircle size={80} /></div><div className="talk-grid">{discussions.slice(0, 3).map((space, index) => <article key={space.id} className={`talk-card talk-${index + 1}`}><span>{space.kind === "city" ? "ГОРОД" : space.kind === "venue" ? "МЕСТО" : "СОБЫТИЕ"}</span><h3>{space.title}</h3><p>{space.posts_count} сообщений</p><button type="button">зайти <ArrowRight size={17} /></button></article>)}</div></section>

        <section className="community-cta"><p className="eyebrow">СОБЫТИЯ ОТ ЛЮДЕЙ</p><h2>Устроили что-то настоящее?</h2><p>Добавьте событие. После проверки администратором оно попадёт в общую ленту и в рекомендации тем, кому действительно подходит.</p><button type="button" onClick={() => { setSubmitState("idle"); setSubmitOpen(true); }}>предложить событие <Plus /></button></section>
      </main>

      <footer><a className="brand footer-brand" href="#top"><img src="/avatar.png" alt="" /><span>ваёбыж</span></a><p>События находят вас.<br />Рекомендации — Кытчи.</p><div><a href="#talks">обсуждения</a><a href="mailto:hello@vayobyzh.ru">hello@vayobyzh.ru</a></div></footer>

      {selected && <div className="modal-backdrop" onMouseDown={(event) => event.target === event.currentTarget && setSelected(null)}><article className="event-modal" role="dialog" aria-modal="true" aria-labelledby="event-modal-title"><button className="modal-close" type="button" onClick={() => setSelected(null)} aria-label="Закрыть"><X /></button><div className={`modal-visual tone-${TONES[selected.id % TONES.length]}`}>{selected.image_url ? <img src={selected.image_url} alt="" /> : <div className="generated-art"><span>{selected.category.slice(0, 1)}</span><i /><b /></div>}<span>{selected.category}</span></div><div className="modal-copy"><p className="eyebrow">КЫТЧИ ПРЕДЛАГАЕТ</p><h2 id="event-modal-title">{selected.title}</h2>{selected.reasons?.length ? <div className="reason-box"><Sparkles /><div><strong>Почему вам</strong><p>{selected.reasons.join(" · ")}</p></div></div> : null}<p className="modal-description">{selected.description}</p><dl><div><dt><Clock3 /> когда</dt><dd>{formatDate(selected.starts_at)}</dd></div><div><dt><MapPin /> где</dt><dd>{selected.venue_name}<small>{selected.address}</small></dd></div><div><dt><Ticket /> вход</dt><dd>{selected.is_free ? "Бесплатно" : selected.price_text || "Цена на сайте"}</dd></div></dl><div className="modal-actions"><button className={plannedIds.includes(selected.id) ? "planned" : ""} type="button" onClick={() => markGoing(selected)}><Heart fill={plannedIds.includes(selected.id) ? "currentColor" : "none"} />{plannedIds.includes(selected.id) ? "Вы идёте" : "Пойду"}</button><a href={selected.event_url || "#"} target="_blank" rel="noreferrer">страница события <ExternalLink /></a></div></div></article></div>}

      {submitOpen && <div className="modal-backdrop" onMouseDown={(event) => event.target === event.currentTarget && setSubmitOpen(false)}><article className="submit-modal" role="dialog" aria-modal="true" aria-labelledby="submit-title"><button className="modal-close" type="button" onClick={() => setSubmitOpen(false)} aria-label="Закрыть"><X /></button>{submitState === "done" ? <div className="submit-done"><Check /><p className="eyebrow">ПРИНЯТО</p><h2>Событие ушло на проверку.</h2><p>После решения администратора вы получите уведомление, а одобренная карточка появится в ленте.</p><button type="button" onClick={() => setSubmitOpen(false)}>понятно</button></div> : <><p className="eyebrow">СОБЫТИЕ ОТ СООБЩЕСТВА</p><h2 id="submit-title">Покажите городу,<br />что вы придумали.</h2><form onSubmit={handleSubmission}><label><span>Название</span><input name="title" required minLength={4} placeholder="Ночь независимого кино" /></label><div className="form-row"><label><span>Дата и время</span><input name="starts_at" type="datetime-local" required /></label><label><span>Категория</span><select name="category"><option>Музыка</option><option>Выставки</option><option>Театр</option><option>Кино</option><option>Лекции</option><option>Другое</option></select></label></div><label><span>Что будет</span><textarea name="description" required minLength={20} placeholder="Расскажите достаточно, чтобы модератор и будущие гости всё поняли." /></label><div className="form-row"><label><span>Площадка</span><input name="venue_name" required placeholder="Название места" /></label><label><span>Адрес</span><input name="address" required placeholder="Улица, дом" /></label></div><div className="form-row"><label><span>Теги через запятую</span><input name="tags" placeholder="кино, встреча, вечер" /></label><label><span>Цена</span><input name="price_text" placeholder="от 500 ₽" /></label></div><label className="checkbox-label"><input type="checkbox" name="is_free" /><span>Вход бесплатный</span></label><button className="send-submission" type="submit" disabled={submitState === "sending"}>{submitState === "sending" ? "отправляем…" : "отправить на модерацию"}<ArrowRight /></button></form></>}</article></div>}
    </div>
  );
}

export default App;
