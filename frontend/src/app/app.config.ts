import { ApplicationConfig, provideBrowserGlobalErrorListeners, isDevMode } from '@angular/core';
import { provideRouter } from '@angular/router';
import { provideHttpClient, withInterceptors } from '@angular/common/http';
import { authErrorInterceptor } from './services/auth-error.interceptor';
import { provideAnimationsAsync } from '@angular/platform-browser/animations/async';
import { providePrimeNG } from 'primeng/config';
import { provideStore } from '@ngrx/store';
import { provideEffects } from '@ngrx/effects';
import { provideStoreDevtools } from '@ngrx/store-devtools';
import Aura from '@primeng/themes/aura';

import { routes } from './app.routes';
import { s3Reducer } from './store/s3.reducer';
import { S3Effects } from './store/s3.effects';
import { datasetReducer } from './store/dataset.reducer';
import { DatasetEffects } from './store/dataset.effects';

export const appConfig: ApplicationConfig = {
  providers: [
    provideBrowserGlobalErrorListeners(),
    provideRouter(routes),
    provideHttpClient(withInterceptors([authErrorInterceptor])),
    provideAnimationsAsync(),
    providePrimeNG({
      theme: {
        preset: Aura,
        options: {
          darkModeSelector: '.dark-mode'
        }
      }
    }),
    provideStore({ s3: s3Reducer, dataset: datasetReducer }),
    provideEffects([S3Effects, DatasetEffects]),
    provideStoreDevtools({ maxAge: 50, logOnly: !isDevMode() }),
  ]
};
