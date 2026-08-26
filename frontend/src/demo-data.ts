import type { City, EventItem } from "./types";

export const demoCities: City[] = [
  { slug: "msk", name: "Москва", timezone: "Europe/Moscow" },
  { slug: "spb", name: "Санкт-Петербург", timezone: "Europe/Moscow" },
  { slug: "ekb", name: "Екатеринбург", timezone: "Asia/Yekaterinburg" },
  { slug: "kzn", name: "Казань", timezone: "Europe/Moscow" },
  { slug: "nsk", name: "Новосибирск", timezone: "Asia/Novosibirsk" },
];

const futureIso = (days: number, hour: number) => {
  const value = new Date();
  value.setDate(value.getDate() + days);
  value.setHours(hour, 0, 0, 0);
  return value.toISOString();
};

export const createDemoEvents = (): EventItem[] => [
  {
    id: 101, source: "demo", city_slug: "msk", title: "Музыка на крыше",
    description: "Летний концерт на закате: неоклассика, электроника и панорама вечернего города.",
    category: "Музыка", tags: ["Музыка", "На улице", "Вечером"], starts_at: futureIso(0, 20), ends_at: futureIso(0, 22),
    venue_name: "Хлебозавод №9", address: "Новодмитровская улица, 1", latitude: null, longitude: null, image_url: "",
    event_url: "https://kudago.com/msk/", price_text: "от 1 200 ₽", is_free: false, age_min: 12, popularity: 485,
    reasons: ["Совпадает с интересом «музыка»", "Популярно у зрителей"],
  },
  {
    id: 102, source: "demo", city_slug: "msk", title: "Город как текст",
    description: "Выставка о вывесках, маршрутах и невидимых историях, из которых складывается образ Москвы.",
    category: "Выставки", tags: ["Выставки", "Архитектура", "Искусство"], starts_at: futureIso(1, 10), ends_at: futureIso(18, 21),
    venue_name: "Музей Москвы", address: "Зубовский бульвар, 2", latitude: null, longitude: null, image_url: "",
    event_url: "https://kudago.com/msk/", price_text: "600 ₽", is_free: false, age_min: 6, popularity: 361,
    reasons: ["Совпадает с интересом «искусство»"],
  },
  {
    id: 103, source: "demo", city_slug: "msk", title: "Почему мы любим города",
    description: "Разговор урбаниста и антрополога о памяти места, привычных маршрутах и чувстве дома.",
    category: "Лекции", tags: ["Лекции", "Город", "Архитектура"], starts_at: futureIso(2, 19), ends_at: futureIso(2, 21),
    venue_name: "Дом культуры ГЭС-2", address: "Болотная набережная, 15", latitude: null, longitude: null, image_url: "",
    event_url: "https://kudago.com/msk/", price_text: "Бесплатно по регистрации", is_free: true, age_min: 16, popularity: 294,
    reasons: ["Новый вариант для разнообразия"],
  },
  {
    id: 104, source: "demo", city_slug: "msk", title: "Вечер короткого метра",
    description: "Шесть новых короткометражных фильмов и обсуждение с авторами после показа.",
    category: "Кино", tags: ["Кино", "Фестиваль", "Вечером"], starts_at: futureIso(4, 18), ends_at: futureIso(4, 22),
    venue_name: "Иллюзион", address: "Котельническая набережная, 1/15", latitude: null, longitude: null, image_url: "",
    event_url: "https://kudago.com/msk/", price_text: "800 ₽", is_free: false, age_min: 18, popularity: 233,
  },
  {
    id: 105, source: "demo", city_slug: "msk", title: "Маркет локальных издательств",
    description: "Независимые журналы, фотокниги, зин-культура и встречи с редакторами.",
    category: "Фестивали", tags: ["Книги", "Маркет", "Искусство"], starts_at: futureIso(5, 12), ends_at: futureIso(5, 20),
    venue_name: "ДК Рассвет", address: "Столярный переулок, 3", latitude: null, longitude: null, image_url: "",
    event_url: "https://kudago.com/msk/", price_text: "Вход свободный", is_free: true, age_min: 0, popularity: 186,
  },
  {
    id: 106, source: "demo", city_slug: "msk", title: "Импровизация: новый состав",
    description: "Театральная импровизация, где зрители задают темы, а актёры создают истории на ходу.",
    category: "Театр", tags: ["Театр", "Юмор", "Импровизация"], starts_at: futureIso(7, 20), ends_at: futureIso(7, 22),
    venue_name: "Практика", address: "Большой Козихинский переулок, 30", latitude: null, longitude: null, image_url: "",
    event_url: "https://kudago.com/msk/", price_text: "от 1 500 ₽", is_free: false, age_min: 16, popularity: 412,
  },
];

