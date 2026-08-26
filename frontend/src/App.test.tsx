import { describe, expect, it } from "vitest";
import { matchesDateFilter } from "./App";

describe("matchesDateFilter", () => {
  const now = new Date("2026-08-25T10:00:00.000Z");

  it("matches today", () => {
    expect(matchesDateFilter("2026-08-25T18:00:00.000Z", "today", now)).toBe(true);
    expect(matchesDateFilter("2026-08-26T18:00:00.000Z", "today", now)).toBe(false);
  });

  it("limits the week to seven days", () => {
    expect(matchesDateFilter("2026-08-31T18:00:00.000Z", "week", now)).toBe(true);
    expect(matchesDateFilter("2026-09-03T18:00:00.000Z", "week", now)).toBe(false);
  });

  it("allows any future date when no date filter is set", () => {
    expect(matchesDateFilter("2030-01-01T18:00:00.000Z", "any", now)).toBe(true);
  });
});
