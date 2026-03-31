import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ActivatedRoute, Router } from '@angular/router';
import { FormsModule } from '@angular/forms';
import { TableModule, TableLazyLoadEvent } from 'primeng/table';
import { ButtonModule } from 'primeng/button';
import { InputTextModule } from 'primeng/inputtext';
import { ApiService } from '../../services/api.service';
import { ColumnInfo, DataPage } from '../../models/dataset.model';

@Component({
  selector: 'app-data-table',
  standalone: true,
  imports: [CommonModule, FormsModule, TableModule, ButtonModule, InputTextModule],
  template: `
    <div class="data-table-view">
      <div class="header">
        <p-button icon="pi pi-arrow-left" label="Back" [text]="true" (onClick)="goBack()" />
        <h2>{{ datasetName }}</h2>
        <span class="row-count">{{ totalRows | number }} rows</span>
      </div>

      <p-table
        [value]="rows"
        [lazy]="true"
        [paginator]="true"
        [rows]="pageSize"
        [totalRecords]="totalRows"
        [loading]="loading"
        [scrollable]="true"
        scrollHeight="calc(100vh - 200px)"
        (onLazyLoad)="onLazyLoad($event)"
        styleClass="p-datatable-sm p-datatable-gridlines p-datatable-striped"
      >
        <ng-template #header>
          <tr>
            @for (col of columns; track col.name) {
              <th [pSortableColumn]="col.name" style="min-width: 120px">
                {{ col.name }}
                <p-sortIcon [field]="col.name" />
              </th>
            }
          </tr>
          <tr>
            @for (col of columns; track col.name) {
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
            @for (col of columns; track col.name) {
              <td>{{ row[col.name] }}</td>
            }
          </tr>
        </ng-template>
        <ng-template #emptymessage>
          <tr><td [attr.colspan]="columns.length" style="text-align: center">No data</td></tr>
        </ng-template>
      </p-table>
    </div>
  `,
  styles: [`
    .header { display: flex; align-items: center; gap: 1rem; margin-bottom: 1rem; }
    .row-count { color: var(--p-text-muted-color); font-size: 0.9rem; }
  `]
})
export class DataTableComponent implements OnInit {
  datasetId!: number;
  datasetName = '';
  columns: ColumnInfo[] = [];
  rows: Record<string, unknown>[] = [];
  totalRows = 0;
  pageSize = 100;
  loading = false;
  filters: Record<string, string> = {};
  private filterTimeout: ReturnType<typeof setTimeout> | null = null;

  constructor(
    private route: ActivatedRoute,
    private router: Router,
    private api: ApiService
  ) {}

  ngOnInit() {
    this.datasetId = +this.route.snapshot.paramMap.get('id')!;
    this.api.getDataset(this.datasetId).subscribe(ds => this.datasetName = ds.name);
    this.api.getSchema(this.datasetId).subscribe(schema => {
      this.columns = schema;
      this.loadData(0);
    });
  }

  onLazyLoad(event: TableLazyLoadEvent) {
    const page = event.first !== undefined ? Math.floor(event.first / this.pageSize) : 0;
    const sortField = event.sortField as string | undefined;
    const sortOrder = event.sortOrder === -1 ? 'DESC' : 'ASC';
    this.loadData(page, sortField, sortOrder);
  }

  onFilter(columnName: string, event: Event) {
    const value = (event.target as HTMLInputElement).value;
    if (value) {
      this.filters[columnName] = value;
    } else {
      delete this.filters[columnName];
    }
    if (this.filterTimeout) clearTimeout(this.filterTimeout);
    this.filterTimeout = setTimeout(() => this.loadData(0), 400);
  }

  loadData(page: number, sortField?: string, sortOrder?: string) {
    this.loading = true;
    this.api.getData(this.datasetId, page, this.pageSize, sortField, sortOrder, this.filters).subscribe({
      next: (result: DataPage) => {
        this.rows = result.data;
        this.totalRows = result.totalRows;
        this.loading = false;
      },
      error: () => { this.loading = false; }
    });
  }

  goBack() {
    this.router.navigate(['/']);
  }
}
