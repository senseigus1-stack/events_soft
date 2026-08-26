export type City = {
  slug: string;
  name: string;
  timezone: string;
};

export type EventItem = {
  id: number;
  source: string;
  city_slug: string;
  title: string;
  description: string;
  category: string;
  tags: string[];
  starts_at: string;
  ends_at: string | null;
  venue_name: string;
  address: string;
  latitude: number | null;
  longitude: number | null;
  image_url: string;
  event_url: string;
  price_text: string;
  is_free: boolean;
  age_min: number | null;
  popularity: number;
  status?: string;
  score?: number;
  reasons?: string[];
};

export type DateFilter = "any" | "today" | "weekend" | "week";

export type Plan = {
  id: number;
  status: string;
  reminder_at: string | null;
  event: EventItem;
};

export type Profile = {
  id: string;
  display_name: string;
  city_slug: string | null;
  avatar_url: string;
};

export type OAuthProvider = {
  id: "google" | "yandex" | "github";
  name: string;
  available: boolean;
};

export type Friend = {
  id: number;
  status: string;
  direction: "incoming" | "outgoing" | "accepted";
  friend: Profile;
  shared_events: EventItem[];
};

export type NotificationItem = {
  id: number;
  event_id: number | null;
  kind: string;
  title: string;
  body: string;
  created_at: string;
  read_at: string | null;
};

export type DiscussionSpace = {
  id: number;
  kind: "city" | "event" | "venue";
  title: string;
  city_slug: string | null;
  event_id: number | null;
  venue_name: string;
  posts_count: number;
};

export type EventSubmission = {
  city_slug: string;
  title: string;
  description: string;
  category: string;
  tags: string[];
  starts_at: string;
  venue_name: string;
  address: string;
  price_text: string;
  is_free: boolean;
};
