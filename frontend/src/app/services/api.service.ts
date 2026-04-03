import { Injectable } from '@angular/core';
import { HttpClient, HttpParams } from '@angular/common/http';
import { Observable } from 'rxjs';
import { Dataset, ColumnInfo, DataPage, ComparisonSummary, ColumnChangeSummary } from '../models/dataset.model';

@Injectable({ providedIn: 'root' })
export class ApiService {
  private baseUrl = '/api';

  constructor(private http: HttpClient) {}

  getHealth(): Observable<{ status: string }> {
    return this.http.get<{ status: string }>(`${this.baseUrl}/health`);
  }

  listDatasets(): Observable<Dataset[]> {
    return this.http.get<Dataset[]>(`${this.baseUrl}/datasets`);
  }

  getDataset(id: number): Observable<Dataset> {
    return this.http.get<Dataset>(`${this.baseUrl}/datasets/${id}`);
  }

  getSchema(id: number): Observable<ColumnInfo[]> {
    return this.http.get<ColumnInfo[]>(`${this.baseUrl}/datasets/${id}/schema`);
  }

  getData(
    id: number,
    page: number = 0,
    size: number = 100,
    sortField?: string,
    sortOrder?: string,
    filters?: Record<string, string>
  ): Observable<DataPage> {
    let params = new HttpParams()
      .set('page', page.toString())
      .set('size', size.toString());

    if (sortField) params = params.set('sortField', sortField);
    if (sortOrder) params = params.set('sortOrder', sortOrder);
    if (filters && Object.keys(filters).length > 0) {
      params = params.set('filters', JSON.stringify(filters));
    }

    return this.http.get<DataPage>(`${this.baseUrl}/datasets/${id}/data`, { params });
  }

  getColumnOrder(id: number): Observable<string[]> {
    return this.http.get<string[]>(`${this.baseUrl}/datasets/${id}/column-order`);
  }

  setColumnOrder(id: number, columnOrder: string[]): Observable<void> {
    return this.http.put<void>(`${this.baseUrl}/datasets/${id}/column-order`, columnOrder);
  }

  getColumnWidths(id: number): Observable<Record<string, number>> {
    return this.http.get<Record<string, number>>(`${this.baseUrl}/datasets/${id}/column-widths`);
  }

  setColumnWidths(id: number, widths: Record<string, number>): Observable<void> {
    return this.http.put<void>(`${this.baseUrl}/datasets/${id}/column-widths`, widths);
  }

  getHiddenColumns(id: number): Observable<string[]> {
    return this.http.get<string[]>(`${this.baseUrl}/datasets/${id}/hidden-columns`);
  }

  setHiddenColumns(id: number, hiddenColumns: string[]): Observable<void> {
    return this.http.put<void>(`${this.baseUrl}/datasets/${id}/hidden-columns`, hiddenColumns);
  }

  getDistinctValues(id: number, column: string, filters?: Record<string, string>, limit: number = 500): Observable<{ value: unknown; count: number }[]> {
    let params = new HttpParams().set('limit', limit);
    if (filters && Object.keys(filters).length > 0) {
      params = params.set('filters', JSON.stringify(filters));
    }
    return this.http.get<{ value: unknown; count: number }[]>(
      `${this.baseUrl}/datasets/${id}/distinct/${encodeURIComponent(column)}`, { params }
    );
  }

  getNullColumns(id: number): Observable<string[]> {
    return this.http.get<string[]>(`${this.baseUrl}/datasets/${id}/null-columns`);
  }

  uploadFile(file: File, name: string): Observable<{ datasetId: number; name: string; rowCount: number }> {
    const formData = new FormData();
    formData.append('file', file);
    formData.append('name', name);
    return this.http.post<{ datasetId: number; name: string; rowCount: number }>(`${this.baseUrl}/datasets/upload`, formData);
  }

  deleteDataset(id: number): Observable<void> {
    return this.http.delete<void>(`${this.baseUrl}/datasets/${id}`);
  }

  // Comparison
  compare(leftId: number, rightId: number, keyColumns: string[], ignoreColumns: string[] = []): Observable<ComparisonSummary> {
    return this.http.post<ComparisonSummary>(`${this.baseUrl}/compare`, {
      leftDatasetId: leftId, rightDatasetId: rightId, keyColumns, ignoreColumns
    });
  }

  getCompareAdded(leftId: number, rightId: number, page: number = 0, size: number = 100): Observable<DataPage> {
    return this.http.get<DataPage>(`${this.baseUrl}/compare/${leftId}/${rightId}/added`, {
      params: new HttpParams().set('page', page).set('size', size)
    });
  }

  getCompareRemoved(leftId: number, rightId: number, page: number = 0, size: number = 100): Observable<DataPage> {
    return this.http.get<DataPage>(`${this.baseUrl}/compare/${leftId}/${rightId}/removed`, {
      params: new HttpParams().set('page', page).set('size', size)
    });
  }

  getCompareChanged(leftId: number, rightId: number, page: number = 0, size: number = 100): Observable<DataPage> {
    return this.http.get<DataPage>(`${this.baseUrl}/compare/${leftId}/${rightId}/changed`, {
      params: new HttpParams().set('page', page).set('size', size)
    });
  }

  getColumnChanges(leftId: number, rightId: number): Observable<ColumnChangeSummary[]> {
    return this.http.get<ColumnChangeSummary[]>(`${this.baseUrl}/compare/${leftId}/${rightId}/column-changes`);
  }

  getColumnChangeData(leftId: number, rightId: number, column: string, page: number = 0, size: number = 100): Observable<DataPage> {
    return this.http.get<DataPage>(`${this.baseUrl}/compare/${leftId}/${rightId}/column/${encodeURIComponent(column)}`, {
      params: new HttpParams().set('page', page).set('size', size)
    });
  }
}
