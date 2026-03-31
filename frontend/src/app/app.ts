import { Component } from '@angular/core';
import { RouterOutlet } from '@angular/router';
import { NotificationBarComponent } from './components/notification-bar/notification-bar';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [RouterOutlet, NotificationBarComponent],
  templateUrl: './app.html',
  styleUrl: './app.scss'
})
export class App {
  title = 'DFViewer';
}
