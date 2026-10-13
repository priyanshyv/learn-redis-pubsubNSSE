export interface RedisConfig {
  host: string;
  port: number;
}

export interface AppConfig {
  /** Deployment environment. Prefixes every Redis channel name — see PHASE 2. */
  env: string;
  /** Which instance this process is, so multi-process demos are legible in logs. */
  instanceName: string;
  port: number;
  redisConfig: RedisConfig;
}

const config: AppConfig = {
  env: process.env.NODE_ENV || 'local',
  instanceName: process.env.INSTANCE_NAME || 'A',
  port: Number(process.env.PORT) || 3001,
  redisConfig: {
    host: process.env.REDIS_HOST || '127.0.0.1',
    port: Number(process.env.REDIS_PORT) || 6379,
  },
};

export default config;
