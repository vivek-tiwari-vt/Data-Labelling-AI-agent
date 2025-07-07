"""
Enhanced Analytics Report Generator - Orchestrates chart generation and PDF building
"""
import json
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime
from io import BytesIO

from .chart_generator import ChartGenerator
from .pdf_builder import PDFBuilder

try:
    REPORTLAB_AVAILABLE = True
except ImportError:
    REPORTLAB_AVAILABLE = False


class EnhancedReportGenerator:
    """Enhanced analytics report generator with visualizations"""
    
    def __init__(self):
        self.data_dir = Path("/Volumes/DATA/Projects/data_label_agent/data")
        self.reports_dir = self.data_dir / "reports"
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        
        self.chart_generator = ChartGenerator()
        self.pdf_builder = PDFBuilder() if REPORTLAB_AVAILABLE else None
    
    def generate_pdf_report(self, analytics_data: Dict[str, Any]) -> bytes:
        """Generate enhanced PDF report with visualizations"""
        if not REPORTLAB_AVAILABLE or not self.pdf_builder:
            return self._generate_fallback_report(analytics_data)
        
        # Generate all charts
        chart_files = self._generate_all_charts(analytics_data)
        
        try:
            # Build the PDF
            pdf_data = self.pdf_builder.create_pdf_document(analytics_data, chart_files)
            return pdf_data
        
        finally:
            # Always cleanup chart files
            ChartGenerator.cleanup_chart_files(chart_files)
    
    def export_data(self, data: Dict[str, Any], format_type: str = "json") -> bytes:
        """Export analytics data in various formats"""
        if format_type.lower() == "pdf":
            return self.generate_pdf_report(data)
        elif format_type.lower() == "json":
            return json.dumps(data, indent=2, default=str).encode('utf-8')
        elif format_type.lower() == "csv":
            return self._export_to_csv(data)
        else:
            raise ValueError(f"Unsupported format: {format_type}")
    
    def _generate_all_charts(self, analytics_data: Dict[str, Any]) -> list:
        """Generate all charts and return list of file paths"""
        chart_files = []
        
        # Job distribution chart
        chart_file = self.chart_generator.create_job_distribution_chart(analytics_data)
        if chart_file:
            chart_files.append(chart_file)
        
        # Processing time chart
        chart_file = self.chart_generator.create_processing_time_chart(analytics_data)
        if chart_file:
            chart_files.append(chart_file)
        
        # Model usage chart
        chart_file = self.chart_generator.create_model_usage_chart(analytics_data)
        if chart_file:
            chart_files.append(chart_file)
        
        # Trend chart
        chart_file = self.chart_generator.create_trend_chart(analytics_data)
        if chart_file:
            chart_files.append(chart_file)
        
        # Quality metrics chart
        chart_file = self.chart_generator.create_quality_metrics_chart(analytics_data)
        if chart_file:
            chart_files.append(chart_file)
        
        return chart_files
    
    def _export_to_csv(self, data: Dict[str, Any]) -> bytes:
        """Export analytics data to CSV format"""
        import csv
        from io import StringIO
        
        output = StringIO()
        
        # Extract key metrics for CSV
        metrics = []
        
        # Basic metrics
        metrics.append({'category': 'General', 'metric': 'total_jobs', 'value': data.get('total_jobs', 0)})
        metrics.append({'category': 'General', 'metric': 'time_period', 'value': data.get('time_period', 'Unknown')})
        
        # Performance metrics
        if data.get('performance_metrics'):
            for key, value in data['performance_metrics'].items():
                if not isinstance(value, dict):
                    metrics.append({'category': 'Performance', 'metric': key, 'value': value})
        
        # Quality metrics
        if data.get('quality_metrics'):
            for key, value in data['quality_metrics'].items():
                if isinstance(value, dict):
                    for subkey, subvalue in value.items():
                        metrics.append({'category': 'Quality', 'metric': f"{key}_{subkey}", 'value': subvalue})
                else:
                    metrics.append({'category': 'Quality', 'metric': key, 'value': value})
        
        # Model analytics
        if data.get('model_analytics', {}).get('model_usage_distribution'):
            for model, count in data['model_analytics']['model_usage_distribution'].items():
                metrics.append({'category': 'Model Usage', 'metric': model, 'value': count})
        
        if metrics:
            writer = csv.DictWriter(output, fieldnames=['category', 'metric', 'value'])
            writer.writeheader()
            writer.writerows(metrics)
        
        return output.getvalue().encode('utf-8')
    
    def _generate_fallback_report(self, analytics_data: Dict[str, Any]) -> bytes:
        """Generate a simple text report when advanced features are not available"""
        report_lines = [
            "Analytics Report",
            "=" * 50,
            f"Generated: {datetime.now().isoformat()}",
            f"Period: {analytics_data.get('time_period', 'Unknown')}",
            "",
            "Summary:",
            f"- Total Jobs: {analytics_data.get('total_jobs', 0)}",
        ]
        
        perf_metrics = analytics_data.get('performance_metrics', {})
        if perf_metrics and not perf_metrics.get('error'):
            report_lines.extend([
                f"- Success Rate: {perf_metrics.get('success_rate', 0):.1f}%",
                f"- Avg Processing Time: {perf_metrics.get('avg_processing_time_ms', 0)/1000:.2f}s",
                f"- Total Texts Processed: {perf_metrics.get('total_texts_processed', 0)}"
            ])
        
        quality_metrics = analytics_data.get('quality_metrics', {})
        if quality_metrics and not quality_metrics.get('error'):
            overall_conf = quality_metrics.get('overall_confidence', {})
            report_lines.extend([
                f"- Average Confidence: {overall_conf.get('average', 0):.3f}",
                f"- Quality Score: {quality_metrics.get('quality_score', 0):.1f}/100"
            ])
        
        model_analytics = analytics_data.get('model_analytics', {})
        if model_analytics and not model_analytics.get('error'):
            model_usage = model_analytics.get('model_usage_distribution', {})
            if model_usage:
                report_lines.append("\nModel Usage:")
                for model, count in sorted(model_usage.items(), key=lambda x: x[1], reverse=True)[:5]:
                    report_lines.append(f"- {model}: {count}")
        
        return "\n".join(report_lines).encode('utf-8')
    
    def save_report(self, analytics_data: Dict[str, Any], filename: Optional[str] = None) -> str:
        """Save analytics report to file and return file path"""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"analytics_report_{timestamp}.pdf"
        
        file_path = self.reports_dir / filename
        
        try:
            pdf_data = self.generate_pdf_report(analytics_data)
            with open(file_path, 'wb') as f:
                f.write(pdf_data)
            return str(file_path)
            
        except Exception as e:
            print(f"Error saving report: {e}")
            # Create a simple text report as fallback
            text_file_path = file_path.with_suffix('.txt')
            with open(text_file_path, 'w') as f:
                fallback_data = self._generate_fallback_report(analytics_data)
                f.write(fallback_data.decode('utf-8'))
            return str(text_file_path)
