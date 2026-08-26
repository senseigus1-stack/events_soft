import type {
  City, DiscussionSpace, EventItem, EventSubmission, Friend, NotificationItem, Plan,
} from "./types";

const API_URL = (import.meta.env.VITE_API_URL ?? "").replace(/\/$/, "");

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const token = localStorage.getItem("vayobyzh-user-token");
  const response = await fetch(`${API_URL}${path}`, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
      ...init?.headers,
    },
  });
  if (!response.ok) throw new Error(`API ${response.status}`);
  if (response.status === 204) return undefined as T;
  return response.json() as Promise<T>;
}

export async function bootstrapSession() {
  const existingId = localStorage.getItem("vayobyzh-user-id");
  const existingToken = localStorage.getItem("vayobyzh-user-token");
  if (existingId && existingToken) return existingId;
  const session = await request<{ user_id: string; token: string }>("/api/v1/auth/session", { method: "POST" });
  localStorage.setItem("vayobyzh-user-id", session.user_id);
  localStorage.setItem("vayobyzh-user-token", session.token);
  return session.user_id;
}

export const fetchCities = () => request<City[]>("/api/v1/cities");

export async function fetchEvents(city: string) {
  const result = await request<{ items: EventItem[] }>(`/api/v1/events?city=${encodeURIComponent(city)}&limit=60`);
  return result.items;
}

export async function fetchRecommendations(userId: string, city: string) {
  const result = await request<{ items: EventItem[] }>(`/api/v1/users/${encodeURIComponent(userId)}/recommendations?city=${encodeURIComponent(city)}&limit=24`);
  return result.items;
}

export const saveInterests = (userId: string, tags: string[]) => request<void>(`/api/v1/users/${encodeURIComponent(userId)}/interests`, { method: "PUT", body: JSON.stringify({ tags }) });

export const recordInteraction = (userId: string, eventId: number, action: "open" | "like" | "save" | "attend" | "dismiss") => request<{ status: string }>(`/api/v1/users/${encodeURIComponent(userId)}/interactions`, { method: "POST", body: JSON.stringify({ event_id: eventId, action }) });

export const fetchPlans = (userId: string) => request<Plan[]>(`/api/v1/users/${encodeURIComponent(userId)}/plans`);

export const setPlan = (userId: string, eventId: number, going = true) => request<Plan>(`/api/v1/users/${encodeURIComponent(userId)}/plans/${eventId}`, {
  method: "PUT",
  body: JSON.stringify({ status: going ? "going" : "cancelled", reminder_minutes_before: 1440 }),
});

export const fetchFriends = (userId: string) => request<Friend[]>(`/api/v1/users/${encodeURIComponent(userId)}/friends`);

export const addFriend = (userId: string, friendUserId: string) => request<{ id: number; status: string }>(`/api/v1/users/${encodeURIComponent(userId)}/friends`, {
  method: "POST",
  body: JSON.stringify({ friend_user_id: friendUserId }),
});

export const fetchNotifications = (userId: string) => request<NotificationItem[]>(`/api/v1/users/${encodeURIComponent(userId)}/notifications`);

export const fetchDiscussions = (city: string) => request<DiscussionSpace[]>(`/api/v1/discussions?city=${encodeURIComponent(city)}`);

export const submitEvent = (userId: string, payload: EventSubmission) => request<EventItem>(`/api/v1/users/${encodeURIComponent(userId)}/event-submissions`, {
  method: "POST",
  body: JSON.stringify(payload),
});
