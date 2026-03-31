import { Component, OnInit, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ActivatedRoute, Router } from '@angular/router';
import { FormsModule } from '@angular/forms';
import { Store } from '@ngrx/store';
import { TableModule, TableLazyLoadEvent } from 'primeng/table';
import { ButtonModule } from 'primeng/button';
import { InputTextModule } from 'primeng/inputtext';
import { MessageModule } from 'primeng/message';
import { DatasetActions } from '../../store/dataset.actions';
import * as Sel from '../../store/dataset.selectors';

@Component({
  selector: 'app-data-table',
  standalone: true,
  imports: [CommonModule, FormsModule, TableModule, ButtonModule, InputTextModule, MessageModule],
  template: `
    <div class="data-table-view">
      <div class="header">
        <p-button icon="pi pi-arrow-left" label="Back" [text]="true" (onClick)="goBack()" />
        <h2>{{ (currentDataset$ | async)?.name }}</h2>
        <span class="row-count">{{ (totalRows$ | async) | number }} rows</span>
      </div>

      @if (error$ | async; as error) {
        <p-message severity="error" [text]="error" />
      } @else {
        <p-table
          [value]="(rows$ | async) ?? []"
          [lazy]="true"
          [paginator]="true"
          [rows]="pageSize"
          [totalRecords]="(totalRows$ | async) ?? 0"
          [loading]="(loading$ | async) ?? false"
          [scrollable]="true"
          scrollHeight="calc(100vh - 200px)"
          (onLazyLoad)="onLazyLoad($event)"
          styleClass="p-datatable-sm p-datatable-gridlines p-datatable-striped"
        >
          <ng-template #header>
            <tr>
              @for (col of (columns$ | async) ?? []; track col.name) {
                <th [pSortableColumn]="col.name" style="min-width: 120px">
                  {{ col.name }}
                  <p-sortIcon [field]="col.name" />
                </th>
              }
            </tr>
            <tr>
              @for (col of (columns$ | async) ?? []; track col.name) {
                <th>
                  <input
                    pInputText
                    type="text"
                    [placeholder]="'Filter ' + col.name"
                    (input)="onFilter(col.name, $event)"
                    style="width: 100%"
                  />
                </th>
              }
            </tr>
          </ng-template>
          <ng-template #body let-row>
            <tr>
              @for (col of (columns$ | async) ?? []; track col.name) {
                <td>{{ row[col.name] }}</td>
              }
            </tr>
          </ng-template>
          <ng-template #emptymessage>
            <tr><td [attr.colspan]="(columns$ | async)?.length ?? 1" style="text-align: center">No data</td></tr>
          </ng-template>
        </p-table>
      }
    </div>
  `,
  styles: [`
    .header { display: flex; align-items: center; gap: 1rem; margin-bottom: 1rem; }
    .row-count { color: var(--p-text-muted-color); font-size: 0.9rem; }
  `]
})
export class DataTableComponent implements OnInit {
  private store = inject(Store);
  private route = inject(ActivatedRoute);
  private router = inject(Router);

  currentDataset$ = this.store.select(Sel.selectCurrentDataset);
  columns$ = this.store.select(Sel.selectColumns);
  rows$ = this.store.select(Sel.selectRows);
  totalRows$ = this.store.select(Sel.selectTotalRows);
  loading$ = this.store.select(Sel.selectDataLoading);
  error$ = this.store.select(Sel.selectDatasetError);

  pageSize = 100;
  filters: Record<string, string> = {};
  private filterTimeout: ReturnType<typeof setTimeout> | null = null;

  ngOnInit() {
    const id = +this.route.snapshot.paramMap.get('id')!;
    this.store.dispatch(DatasetActions.openDataset({ id }));
  }

  onLazyLoad(event: TableLazyLoadEvent) {
    const page = event.first !== undefined ? Math.floor(event.first / this.pageSize) : 0;
    const sortField = event.sortField as string | undefined;
    const sortOrder = event.sortOrder === -1 ? 'DESC' : 'ASC';
    this.store.dispatch(DatasetActions.loadData({
      page, size: this.pageSize, sortField, sortOrder, filters: Object.keys(this.filters).length > 0 ? this.filters : undefined
    }));
  }

  onFilter(columnName: string, event: Event) {
    const value = (event.target as HTMLInputElement).value;
    if (value) {
      this.filters[columnName] = value;
    } else {
      delete this.filters[columnName];
    }
    if (this.filterTimeout) clearTimeout(this.filterTimeout);
    this.filterTimeout = setTimeout(() => {
      this.store.dispatch(DatasetActions.loadData({
        page: 0, size: this.pageSize, filters: Object.keys(this.filters).length > 0 ? this.filters : undefined
      }));
    }, 400);
  }

  goBack() {
    this.router.navigate(['/']);
  }
}
