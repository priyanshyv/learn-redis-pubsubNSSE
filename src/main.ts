import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { ValidationPipe } from '@nestjs/common';
import { AppModule } from './app.module';
import { markShuttingDown } from './shutdown.state';
import config from '../config/config';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  app.useGlobalPipes(new ValidationPipe({ transform: true, whitelist: true }));

  // enableShutdownHooks is what makes Nest call onModuleDestroy on SIGTERM.
  // Without it the process exits with Redis connections still open.
  app.enableShutdownHooks();

  // Flip the flag BEFORE Nest starts tearing modules down, so in-flight work
  // sees it. Nest's own hooks run after this listener.
  ['SIGTERM', 'SIGINT'].forEach((signal) => {
    process.on(signal, () => markShuttingDown());
  });

  await app.listen(config.port);
  console.log(
    `[${config.instanceName}] listening on :${config.port} (env=${config.env})`,
  );
}

void bootstrap();
