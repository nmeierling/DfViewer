import { HttpInterceptorFn, HttpErrorResponse } from '@angular/common/http';
import { inject } from '@angular/core';
import { catchError, throwError } from 'rxjs';
import { NotificationService } from './notification.service';

export const authErrorInterceptor: HttpInterceptorFn = (req, next) => {
  const notifications = inject(NotificationService);

  return next(req).pipe(
    catchError((error: HttpErrorResponse) => {
      if (error.status === 401 || error.error?.error === 'TOKEN_EXPIRED') {
        notifications.show({
          id: 'token-expired',
          message: 'AWS credentials expired',
          detail: 'Please reconfigure your S3 credentials',
          type: 'error'
        });
        // Dispatch a custom event so the S3 config component can react
        window.dispatchEvent(new CustomEvent('s3-token-expired'));
      }
      return throwError(() => error);
    })
  );
};
