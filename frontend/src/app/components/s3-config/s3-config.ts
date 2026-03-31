import { Component, OnInit, OnDestroy, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Store } from '@ngrx/store';
import { InputTextModule } from 'primeng/inputtext';
import { TextareaModule } from 'primeng/textarea';
import { ButtonModule } from 'primeng/button';
import { FloatLabelModule } from 'primeng/floatlabel';
import { MessageModule } from 'primeng/message';
import { PopoverModule } from 'primeng/popover';
import { S3Actions } from '../../store/s3.actions';
import { selectConfigured, selectRegion, selectTokenExpired, selectConfiguring } from '../../store/s3.selectors';
import { S3Credentials } from '../../models/s3.model';
import { S3StateService } from '../../services/s3-state.service';

@Component({
  selector: 'app-s3-config',
  standalone: true,
  imports: [CommonModule, FormsModule, InputTextModule, TextareaModule, ButtonModule, FloatLabelModule, MessageModule, PopoverModule],
  template: `
    <p-button
      [icon]="(configured$ | async) && !(tokenExpired$ | async) ? 'pi pi-lock' : 'pi pi-lock-open'"
      [label]="(configured$ | async) && !(tokenExpired$ | async) ? 'S3: ' + (region$ | async) : 'Configure S3'"
      [severity]="(tokenExpired$ | async) ? 'danger' : (configured$ | async) ? 'success' : 'secondary'"
      [outlined]="true"
      size="small"
      (onClick)="op.toggle($event)"
    />

    <p-popover #op [style]="{ width: '500px' }">
      <div class="config-form">
        @if (tokenExpired$ | async) {
          <p-message severity="error" text="AWS credentials have expired. Please reconfigure." />
        }

        <div class="field">
          <label for="pasteArea" class="paste-label">Paste environment variables</label>
          <textarea
            pTextarea
            id="pasteArea"
            [(ngModel)]="pasteText"
            (ngModelChange)="parsePaste()"
            rows="4"
            placeholder="export AWS_ACCESS_KEY_ID=&quot;AKIA...&quot;&#10;export AWS_SECRET_ACCESS_KEY=&quot;...&quot;&#10;export AWS_SESSION_TOKEN=&quot;...&quot;"
            style="width: 100%; font-family: monospace; font-size: 0.85rem"
          ></textarea>
          @if (parseMessage) {
            <small class="parse-message">{{ parseMessage }}</small>
          }
        </div>

        <div class="divider"><span>or fill manually</span></div>

        <div class="field">
          <p-floatlabel>
            <input pInputText id="accessKey" [(ngModel)]="credentials.accessKeyId" style="width: 100%" />
            <label for="accessKey">AWS Access Key ID</label>
          </p-floatlabel>
        </div>
        <div class="field">
          <p-floatlabel>
            <input pInputText id="secretKey" [(ngModel)]="credentials.secretAccessKey" type="password" style="width: 100%" />
            <label for="secretKey">AWS Secret Access Key</label>
          </p-floatlabel>
        </div>
        <div class="field">
          <p-floatlabel>
            <input pInputText id="sessionToken" [(ngModel)]="credentials.sessionToken" type="password" style="width: 100%" />
            <label for="sessionToken">AWS Session Token (optional)</label>
          </p-floatlabel>
        </div>
        <div class="field">
          <p-floatlabel>
            <input pInputText id="region" [(ngModel)]="credentials.region" style="width: 100%" />
            <label for="region">Region</label>
          </p-floatlabel>
        </div>
        <p-button label="Configure" icon="pi pi-check" (onClick)="save(op)" [loading]="(configuring$ | async) ?? false" />
      </div>
    </p-popover>
  `,
  styles: [`
    .config-form { display: flex; flex-direction: column; gap: 1.25rem; }
    .field { width: 100%; }
    .paste-label { font-weight: 500; margin-bottom: 0.5rem; display: block; }
    .parse-message { color: var(--p-primary-color); }
    .divider {
      display: flex; align-items: center; gap: 0.75rem; color: var(--p-text-muted-color); font-size: 0.85rem;
      &::before, &::after { content: ''; flex: 1; border-top: 1px solid var(--p-surface-border); }
    }
  `]
})
export class S3ConfigComponent implements OnInit, OnDestroy {
  private store = inject(Store);

  configured$ = this.store.select(selectConfigured);
  region$ = this.store.select(selectRegion);
  tokenExpired$ = this.store.select(selectTokenExpired);
  configuring$ = this.store.select(selectConfiguring);

  private tokenExpiredHandler = () => this.store.dispatch(S3Actions.tokenExpired());

  credentials: S3Credentials = {
    accessKeyId: '',
    secretAccessKey: '',
    sessionToken: '',
    region: 'eu-central-1'
  };
  pasteText = '';
  parseMessage = '';

  ngOnInit() {
    window.addEventListener('s3-token-expired', this.tokenExpiredHandler);

    const saved = S3StateService.getSavedCredentials();
    if (saved) this.credentials = saved;

    this.store.dispatch(S3Actions.checkStatus());
  }

  ngOnDestroy() {
    window.removeEventListener('s3-token-expired', this.tokenExpiredHandler);
  }

  parsePaste() {
    if (!this.pasteText.trim()) { this.parseMessage = ''; return; }
    const vars = this.parseEnvVars(this.pasteText);
    let found = 0;
    if (vars['AWS_ACCESS_KEY_ID']) { this.credentials.accessKeyId = vars['AWS_ACCESS_KEY_ID']; found++; }
    if (vars['AWS_SECRET_ACCESS_KEY']) { this.credentials.secretAccessKey = vars['AWS_SECRET_ACCESS_KEY']; found++; }
    if (vars['AWS_SESSION_TOKEN']) { this.credentials.sessionToken = vars['AWS_SESSION_TOKEN']; found++; }
    if (vars['AWS_REGION'] || vars['AWS_DEFAULT_REGION']) {
      this.credentials.region = vars['AWS_REGION'] || vars['AWS_DEFAULT_REGION']; found++;
    }
    this.parseMessage = found > 0 ? `Parsed ${found} variable${found > 1 ? 's' : ''}` : 'No AWS variables found';
  }

  private parseEnvVars(text: string): Record<string, string> {
    const result: Record<string, string> = {};
    for (const line of text.split('\n')) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith('#')) continue;
      const match = trimmed.match(/^(?:export\s+)?(\w+)=(?:"([^"]*)"|'([^']*)'|(.*))/);
      if (match) {
        result[match[1]] = (match[2] ?? match[3] ?? match[4] ?? '').trim();
      }
    }
    return result;
  }

  save(popover?: any) {
    this.store.dispatch(S3Actions.configure({ credentials: this.credentials }));
    // Close popover on success
    this.configured$.subscribe(c => { if (c) popover?.hide(); }).unsubscribe;
    setTimeout(() => popover?.hide(), 500);
  }
}
