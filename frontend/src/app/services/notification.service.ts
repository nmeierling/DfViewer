import { Injectable } from '@angular/core';
import { BehaviorSubject } from 'rxjs';

export interface Notification {
  id: string;
  message: string;
  detail?: string;
  type: 'info' | 'success' | 'error';
  progress?: {
    mode: 'determinate' | 'indeterminate';
    value?: number;
  };
}

@Injectable({ providedIn: 'root' })
export class NotificationService {
  private notifications$ = new BehaviorSubject<Notification[]>([]);
  readonly notifications = this.notifications$.asObservable();

  show(notification: Notification) {
    const current = this.notifications$.value.filter(n => n.id !== notification.id);
    this.notifications$.next([...current, notification]);
  }

  update(id: string, changes: Partial<Notification>) {
    const current = this.notifications$.value;
    const updated = current.map(n => n.id === id ? { ...n, ...changes } : n);
    this.notifications$.next(updated);
  }

  dismiss(id: string) {
    this.notifications$.next(this.notifications$.value.filter(n => n.id !== id));
  }

  dismissAfterDelay(id: string, ms: number = 3000) {
    setTimeout(() => this.dismiss(id), ms);
  }
}
