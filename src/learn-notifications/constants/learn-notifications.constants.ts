import config from '../../../config/config';

/**
 * Every channel name is env-scoped. Local, stage and prod can point at the same
 * Redis; an unprefixed channel means a local dev's publish is delivered to prod
 * subscribers, and pub/sub gives you no way to detect or undo that.
 *
 * Mirrors the CACHE_KEYS convention: a constant or a function returning a
 * prefixed string, never an inline literal at the call site.
 */
const CHANNEL_PREFIX = `${config.env}:learn-notifications`;

export const LEARN_NOTIFICATION_CHANNELS = {
  /** Broadcast to every listener in this environment. */
  ALL: `${CHANNEL_PREFIX}:all`,

  /** One channel per user. Cheap: a channel with no subscriber costs nothing. */
  USER: (userId: number): string => `${CHANNEL_PREFIX}:user:${userId}`,

  /**
   * Matches every USER channel above. Only for tooling that genuinely wants
   * all users — see PHASE 2 on why this is a trap for normal delivery.
   */
  USER_PATTERN: `${CHANNEL_PREFIX}:user:*`,
};
