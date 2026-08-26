import {
  ArrowRight, Bell, CalendarDays, Check, ChevronRight, Clock3, ExternalLink,
  Heart, Languages, LogIn, MapPin, MessageCircle, Plus, RotateCcw, Send, Sparkles,
  Ticket, UserPlus, Users, X, Zap,
} from "lucide-react";
import { useEffect, useMemo, useRef, useState } from "react";
import {
  addFriend, bootstrapSession, fetchAuthProviders, fetchCities, fetchDiscussions,
  fetchEvents, fetchFriends, fetchNotifications, fetchPlans, fetchProfile,
  fetchRecommendations, recordInteraction, saveInterests, setPlan, signOut,
  startOAuth, submitEvent,
} from "./api";
import { createDemoEvents, demoCities } from "./demo-data";
import { INTERESTS, LANGUAGES, LOCALES, TRANSLATIONS, type Language } from "./i18n";
import type {
  City, DateFilter, DiscussionSpace, EventItem, EventSubmission, Friend,
  NotificationItem, OAuthProvider, Profile,
} from "./types";

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

function formatDate(value: string, language: Language) {
  return new Intl.DateTimeFormat(LOCALES[language], {
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
  const copy = TRANSLATIONS[language];
  const authenticated = Boolean(localStorage.getItem("vayobyzh-auth-provider"));

  useEffect(() => {
    document.documentElement.lang = language;
    localStorage.setItem("vayobyzh-language", language);
  }, [language]);

  useEffect(() => {
    let active = true;
    bootstrapSession().then((id) => {
      if (!active) return;
      setUserId(id);
      return Promise.allSettled([fetchProfile(id), fetchAuthProviders()]);
    }).then((results) => {
      if (!active || !results) return;
      if (results[0].status === "fulfilled") setProfile(results[0].value);
      if (results[1].status === "fulfilled") setProviders(results[1].value);
    }).catch(() => undefined);
    return () => { active = false; };
  }, []);

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
    if (!selected && !submitOpen && !authOpen) return;
    document.body.classList.add("modal-open");
    const close = (event: KeyboardEvent) => {
      if (event.key === "Escape") { setSelected(null); setSubmitOpen(false); setAuthOpen(false); }
    };
    window.addEventListener("keydown", close);
    return () => { document.body.classList.remove("modal-open"); window.removeEventListener("keydown", close); };
  }, [selected, submitOpen, authOpen]);

  const current = events[0];
  const next = events[1];
  const cityName = cities.find((item) => item.slug === city)?.name ?? copy.yourCity;
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

  const handleOAuth = async (provider: OAuthProvider) => {
    if (!provider.available) return;
    setAuthError(false);
    try { await startOAuth(provider.id, userId); }
    catch { setAuthError(true); }
  };

  return (
    <div className="app-shell" id="top">
      <header className="site-header">
        <a className="brand" href="#top" aria-label={copy.brandHome}><img src="/avatar.png" alt="" /><span>ваёбыж</span><i>beta</i></a>
        <nav aria-label={copy.talks}><a href="#kytchi">{copy.kytchi}</a><a href="#plans">{copy.plans}</a><a href="#friends">{copy.friends}</a><a href="#talks">{copy.talks}</a></nav>
        <div className="header-tools">
          <label className="language-picker" title={copy.language}><Languages size={17} /><span>{LANGUAGES.find((item) => item.id === language)?.short}</span><select value={language} onChange={(event) => setLanguage(event.target.value as Language)} aria-label={copy.language}>{LANGUAGES.map((item) => <option key={item.id} value={item.id}>{item.label}</option>)}</select></label>
          <button className="auth-button" type="button" onClick={() => setAuthOpen(true)}><LogIn size={17} /><span>{authenticated ? profile?.display_name || copy.account : copy.signIn}</span></button>
          <button className="submit-event-button" type="button" onClick={() => { setSubmitState("idle"); setSubmitOpen(true); }}><Plus size={18} /> {copy.ownEvent}</button>
        </div>
      </header>

      <main>
        <section className="hero" id="kytchi">
          <div className="hero-word"><span>{copy.heroSwipe}</span><strong>{copy.heroBoredom}</strong></div>
          <div className="hero-side"><p><Zap size={17} /> {copy.heroKicker}</p><h1>{copy.heroLead}</h1><label className="city-picker"><MapPin size={18} /><span>{cityName}</span><select value={city} onChange={(event) => setCity(event.target.value)} aria-label={copy.city}>{cities.map((item) => <option key={item.slug} value={item.slug}>{item.name}</option>)}</select></label></div>
        </section>

        <section className="swipe-zone" aria-labelledby="swipe-title">
          <aside className="swipe-aside"><p className="eyebrow">{copy.kytchiEyebrow}</p><h2 id="swipe-title">{copy.swipeTitle}</h2><p>{copy.swipeBody}</p><button className="tune-button" type="button" onClick={() => setInterestOpen((value) => !value)}><Sparkles size={17} /> {copy.tune} <ChevronRight size={17} /></button>{demoMode && <span className="demo-badge">{copy.demo}</span>}</aside>
          <div className="deck-wrap">
            {loading ? <div className="deck-loading"><span /><p>{copy.loading}</p></div> : current ? <>
              {next && <article className={`swipe-card next-card tone-${TONES[next.id % TONES.length]}`} aria-hidden="true"><div /></article>}
              <article className={`swipe-card active-card tone-${TONES[current.id % TONES.length]} ${dragging ? "is-dragging" : ""}`} style={{ transform: `translateX(${dragX}px) rotate(${dragX / 24}deg)` }} onPointerDown={(event) => { pointerStart.current = event.clientX; setDragging(true); event.currentTarget.setPointerCapture(event.pointerId); }} onPointerMove={(event) => dragging && setDragX(event.clientX - pointerStart.current)} onPointerUp={() => { setDragging(false); if (dragX < -90) dismissCurrent(); else if (dragX > 90) showDetails(); else setDragX(0); }}>
                <div className="swipe-art">{current.image_url ? <img src={current.image_url} alt="" draggable="false" /> : <div className="generated-art"><span>{current.category.slice(0, 1)}</span><i /><b /></div>}<span className="card-counter">{String(initialEvents.length - events.length + 1).padStart(2, "0")} / {String(initialEvents.length).padStart(2, "0")}</span><span className="card-category">{current.category}</span>{dragX < -35 && <strong className="gesture-label no-label">{copy.notMine}</strong>}{dragX > 35 && <strong className="gesture-label yes-label">{copy.look}</strong>}</div>
                <div className="swipe-copy" onClick={() => showDetails()}><p className="event-date"><CalendarDays size={16} /> {formatDate(current.starts_at, language)}</p><h3>{current.title}</h3><p className="event-place"><MapPin size={16} /> {current.venue_name}</p>{current.reasons?.[0] && <span className="kytchi-reason"><Sparkles size={15} /> {current.reasons[0]}</span>}</div>
              </article>
              <div className="swipe-actions"><button className="reject-action" type="button" onClick={dismissCurrent} aria-label={copy.dislike}><X /></button><button className="details-action" type="button" onClick={() => showDetails()}><span>{copy.details}</span><ArrowRight /></button><button className={`going-action ${plannedIds.includes(current.id) ? "is-going" : ""}`} type="button" onClick={() => markGoing(current)} aria-label={copy.going}><Heart fill={plannedIds.includes(current.id) ? "currentColor" : "none"} /></button></div>
            </> : <div className="deck-empty"><RotateCcw size={34} /><h3>{copy.feedDone}</h3><p>{copy.feedLearned}</p><button type="button" onClick={() => setEvents(initialEvents)}>{copy.again}</button></div>}
          </div>
        </section>

        {interestOpen && <section className="interest-ribbon"><div><p className="eyebrow">{copy.signals}</p><h2>{copy.whatInterests}</h2></div><div className="interest-chips">{INTERESTS.map((item) => <button key={item.value} type="button" className={interests.includes(item.value) ? "selected" : ""} onClick={() => setInterests((values) => values.includes(item.value) ? values.filter((value) => value !== item.value) : [...values, item.value])}>{interests.includes(item.value) && <Check size={16} />}{copy[item.key]}</button>)}</div><button className="save-taste" type="button" onClick={applyInterests}>{copy.rebuild} <ArrowRight size={18} /></button></section>}

        <section className="plans-section" id="plans"><div className="section-title"><div><p className="eyebrow">{copy.plansEyebrow}</p><h2>{copy.plansTitle}</h2></div><p>{copy.plansBody}</p></div>{notifications.filter((item) => !item.read_at).slice(0, 1).map((item) => <div className="notification" key={item.id}><Bell size={19} /><strong>{item.title}</strong><span>{item.body}</span></div>)}<div className="plans-track">{plannedEvents.length ? plannedEvents.map((event) => <button className="plan-card" type="button" key={event.id} onClick={() => showDetails(event)}><span>{formatDate(event.starts_at, language)}</span><strong>{event.title}</strong><small>{event.venue_name}</small><ChevronRight /></button>) : <div className="plan-placeholder"><Heart /><p>{copy.plansEmpty}</p></div>}</div></section>

        <section className="friends-section" id="friends"><div className="section-title"><div><p className="eyebrow">{copy.peopleEyebrow}</p><h2>{copy.peopleTitle}</h2></div><p>{copy.peopleBody}</p></div><div className="friend-layout"><div className="friend-list">{friends.slice(0, 6).map((item, index) => <article className="friend-card" key={item.id}><span className={`friend-avatar friend-tone-${index % 4}`}>{item.friend.display_name.slice(0, 1).toUpperCase()}</span><div><strong>{item.friend.display_name || copy.newFriend}</strong><p>{item.direction === "incoming" ? copy.wantsFriend : item.status === "pending" ? copy.requestSent : item.shared_events[0] ? `${copy.going}: ${item.shared_events[0].title}` : copy.noPlans}</p></div>{item.direction === "incoming" ? <button type="button">{copy.accepts}</button> : <Users size={18} />}</article>)}</div><div className="add-friend-card"><UserPlus size={28} /><h3>{copy.addFriendTitle}</h3><p>{copy.addFriendBody}</p><div><input value={friendCode} onChange={(event) => setFriendCode(event.target.value)} placeholder={copy.friendExample} aria-label={copy.friendCode} /><button type="button" onClick={handleFriend}><Send size={18} /></button></div><small>{copy.yourCode}: {userId.slice(0, 8)}</small></div></div></section>

        <section className="talks-section" id="talks"><div className="talks-head"><div><p className="eyebrow">{copy.talksEyebrow}</p><h2>{copy.talksTitle}</h2></div><MessageCircle size={80} /></div><div className="talk-grid">{discussions.slice(0, 3).map((space, index) => <article key={space.id} className={`talk-card talk-${index + 1}`}><span>{space.kind === "city" ? copy.town : space.kind === "venue" ? copy.venue : copy.event}</span><h3>{space.title}</h3><p>{space.posts_count} {copy.messages}</p><button type="button">{copy.enter} <ArrowRight size={17} /></button></article>)}</div></section>

        <section className="community-cta"><p className="eyebrow">{copy.communityEyebrow}</p><h2>{copy.communityTitle}</h2><p>{copy.communityBody}</p><button type="button" onClick={() => { setSubmitState("idle"); setSubmitOpen(true); }}>{copy.propose} <Plus /></button></section>
      </main>

      <footer><a className="brand footer-brand" href="#top"><img src="/avatar.png" alt="" /><span>ваёбыж</span></a><p>{copy.footerLine}</p><div><a href="#talks">{copy.talks}</a><a href="mailto:hello@vayobyzh.ru">hello@vayobyzh.ru</a></div></footer>

      {selected && <div className="modal-backdrop" onMouseDown={(event) => event.target === event.currentTarget && setSelected(null)}><article className="event-modal" role="dialog" aria-modal="true" aria-labelledby="event-modal-title"><button className="modal-close" type="button" onClick={() => setSelected(null)} aria-label={copy.close}><X /></button><div className={`modal-visual tone-${TONES[selected.id % TONES.length]}`}>{selected.image_url ? <img src={selected.image_url} alt="" /> : <div className="generated-art"><span>{selected.category.slice(0, 1)}</span><i /><b /></div>}<span>{selected.category}</span></div><div className="modal-copy"><p className="eyebrow">{copy.kytchiSuggests}</p><h2 id="event-modal-title">{selected.title}</h2>{selected.reasons?.length ? <div className="reason-box"><Sparkles /><div><strong>{copy.whyYou}</strong><p>{selected.reasons.join(" · ")}</p></div></div> : null}<p className="modal-description">{selected.description}</p><dl><div><dt><Clock3 /> {copy.when}</dt><dd>{formatDate(selected.starts_at, language)}</dd></div><div><dt><MapPin /> {copy.where}</dt><dd>{selected.venue_name}<small>{selected.address}</small></dd></div><div><dt><Ticket /> {copy.admission}</dt><dd>{selected.is_free ? copy.free : selected.price_text || copy.sitePrice}</dd></div></dl><div className="modal-actions"><button className={plannedIds.includes(selected.id) ? "planned" : ""} type="button" onClick={() => markGoing(selected)}><Heart fill={plannedIds.includes(selected.id) ? "currentColor" : "none"} />{plannedIds.includes(selected.id) ? copy.youGo : copy.going}</button><a href={selected.event_url || "#"} target="_blank" rel="noreferrer">{copy.eventPage} <ExternalLink /></a></div></div></article></div>}

      {submitOpen && <div className="modal-backdrop" onMouseDown={(event) => event.target === event.currentTarget && setSubmitOpen(false)}><article className="submit-modal" role="dialog" aria-modal="true" aria-labelledby="submit-title"><button className="modal-close" type="button" onClick={() => setSubmitOpen(false)} aria-label={copy.close}><X /></button>{submitState === "done" ? <div className="submit-done"><Check /><p className="eyebrow">{copy.accepted}</p><h2>{copy.sentTitle}</h2><p>{copy.sentBody}</p><button type="button" onClick={() => setSubmitOpen(false)}>{copy.understood}</button></div> : <><p className="eyebrow">{copy.submissionEyebrow}</p><h2 id="submit-title">{copy.submissionTitle}</h2><form onSubmit={handleSubmission}><label><span>{copy.name}</span><input name="title" required minLength={4} placeholder={copy.nameHint} /></label><div className="form-row"><label><span>{copy.dateTime}</span><input name="starts_at" type="datetime-local" required /></label><label><span>{copy.category}</span><select name="category"><option value="Музыка">{copy.music}</option><option value="Выставки">{copy.exhibitions}</option><option value="Театр">{copy.theatre}</option><option value="Кино">{copy.cinema}</option><option value="Лекции">{copy.lectures}</option><option value="Другое">{copy.other}</option></select></label></div><label><span>{copy.description}</span><textarea name="description" required minLength={20} placeholder={copy.descriptionHint} /></label><div className="form-row"><label><span>{copy.venueName}</span><input name="venue_name" required placeholder={copy.venueHint} /></label><label><span>{copy.address}</span><input name="address" required placeholder={copy.addressHint} /></label></div><div className="form-row"><label><span>{copy.tags}</span><input name="tags" placeholder={copy.tagsHint} /></label><label><span>{copy.price}</span><input name="price_text" placeholder={copy.priceHint} /></label></div><label className="checkbox-label"><input type="checkbox" name="is_free" /><span>{copy.freeEntry}</span></label><button className="send-submission" type="submit" disabled={submitState === "sending"}>{submitState === "sending" ? copy.sending : copy.sendModeration}<ArrowRight /></button></form></>}</article></div>}

      {authOpen && <div className="modal-backdrop" onMouseDown={(event) => event.target === event.currentTarget && setAuthOpen(false)}><article className="auth-modal" role="dialog" aria-modal="true" aria-labelledby="auth-title"><button className="modal-close" type="button" onClick={() => setAuthOpen(false)} aria-label={copy.close}><X /></button><p className="eyebrow">{authenticated ? copy.account : copy.guestMode}</p><h2 id="auth-title">{authenticated ? profile?.display_name || copy.account : copy.loginTitle}</h2>{profile?.avatar_url && <img className="profile-avatar" src={profile.avatar_url} alt="" referrerPolicy="no-referrer" />}<p>{copy.loginBody}</p>{language === "udm" && <p className="translation-note">{copy.udmurtBeta}</p>}<div className="provider-list">{providers.map((provider) => <button key={provider.id} type="button" disabled={!provider.available} onClick={() => void handleOAuth(provider)}><span className={`provider-mark provider-${provider.id}`}>{provider.id === "google" ? "G" : provider.id === "yandex" ? "Я" : "GH"}</span><strong>{provider.name}</strong>{!provider.available && <small>{copy.providerUnavailable}</small>}<ArrowRight /></button>)}</div>{authError && <p className="auth-error">{copy.loginError}</p>}{authenticated && <button className="logout-button" type="button" onClick={signOut}>{copy.logout}</button>}</article></div>}
    </div>
  );
}

export default App;
