"""
PDF Builder - Builds PDF documents with content and visualizations
"""
from typing import Dict, Any, List, Optional
from datetime import datetime
from io import BytesIO

try:
    from reportlab.lib.pagesizes import A4, letter
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image, PageBreak
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib import colors
    from reportlab.lib.units import inch
    REPORTLAB_AVAILABLE = True
except ImportError:
    REPORTLAB_AVAILABLE = False


class PDFBuilder:
    """Builds PDF documents with analytics content"""
    
    def __init__(self):
        if not REPORTLAB_AVAILABLE:
            raise ImportError("ReportLab is required for PDF generation")
        
        self.styles = getSampleStyleSheet()
        self._setup_custom_styles()
    
    def _setup_custom_styles(self):
        """Setup custom paragraph styles"""
        self.title_style = ParagraphStyle(
            'CustomTitle',
            parent=self.styles['Heading1'],
            fontSize=24,
            spaceAfter=20,
            textColor=colors.HexColor('#2C3E50'),
            alignment=1  # Center alignment
        )
        
        self.subtitle_style = ParagraphStyle(
            'CustomSubtitle',
            parent=self.styles['Heading2'],
            fontSize=18,
            spaceAfter=15,
            textColor=colors.HexColor('#34495E'),
            spaceBefore=15
        )
        
        self.section_style = ParagraphStyle(
            'SectionStyle',
            parent=self.styles['Heading3'],
            fontSize=14,
            spaceAfter=10,
            textColor=colors.HexColor('#2980B9'),
            spaceBefore=20
        )
    
    def create_pdf_document(self, analytics_data: Dict[str, Any], chart_files: List[str]) -> bytes:
        """Create a complete PDF document"""
        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=A4, 
                               leftMargin=72, rightMargin=72,
                               topMargin=72, bottomMargin=72)
        story = []
        
        # Title page
        story.extend(self._create_title_page(analytics_data))
        
        # Executive summary
        story.extend(self._create_executive_summary(analytics_data))
        
        # Charts section
        if chart_files:
            story.extend(self._create_charts_section(chart_files))
        
        # Detailed metrics
        story.extend(self._create_detailed_metrics(analytics_data))
        
        # Recommendations
        story.extend(self._create_recommendations(analytics_data))
        
        # Build the PDF
        doc.build(story)
        pdf_data = buffer.getvalue()
        buffer.close()
        
        return pdf_data
    
    def _create_title_page(self, analytics_data: Dict[str, Any]) -> List:
        """Create the title page"""
        elements = []
        
        # Main title
        elements.append(Paragraph("Analytics Report", self.title_style))
        elements.append(Spacer(1, 30))
        
        # Metadata table
        current_time = datetime.now().strftime("%B %d, %Y at %I:%M %p")
        metadata = [
            ['Report Details', ''],
            ['Generated', current_time],
            ['Time Period', analytics_data.get('time_period', 'Unknown')],
            ['Total Jobs', str(analytics_data.get('total_jobs', 0))],
        ]
        
        metadata_table = self._create_styled_table(metadata, col_widths=[2.5*inch, 3*inch])
        elements.append(metadata_table)
        elements.append(PageBreak())
        
        return elements
    
    def _create_executive_summary(self, analytics_data: Dict[str, Any]) -> List:
        """Create executive summary section"""
        elements = []
        elements.append(Paragraph("Executive Summary", self.subtitle_style))
        
        total_jobs = analytics_data.get('total_jobs', 0)
        
        if total_jobs == 0:
            return self._create_no_data_section(analytics_data)
        
        # Summary table
        summary_data = [['Metric', 'Value']]
        
        # Add basic metrics
        summary_data.append(['Total Jobs', str(total_jobs)])
        
        # Performance metrics
        perf_metrics = analytics_data.get('performance_metrics', {})
        if perf_metrics and not perf_metrics.get('error'):
            summary_data.extend([
                ['Success Rate', f"{perf_metrics.get('success_rate', 0):.1f}%"],
                ['Avg Processing Time', f"{perf_metrics.get('avg_processing_time_ms', 0)/1000:.2f}s"],
                ['Total Texts Processed', str(perf_metrics.get('total_texts_processed', 0))]
            ])
        
        # Quality metrics
        quality_metrics = analytics_data.get('quality_metrics', {})
        if quality_metrics and not quality_metrics.get('error'):
            overall_conf = quality_metrics.get('overall_confidence', {})
            summary_data.extend([
                ['Average Confidence', f"{overall_conf.get('average', 0):.3f}"],
                ['Quality Score', f"{quality_metrics.get('quality_score', 0):.1f}/100"]
            ])
        
        summary_table = self._create_styled_table(summary_data, col_widths=[3*inch, 2*inch])
        elements.append(summary_table)
        elements.append(Spacer(1, 20))
        
        return elements
    
    def _create_no_data_section(self, analytics_data: Dict[str, Any]) -> List:
        """Create section for when no data is available"""
        elements = []
        
        elements.append(Paragraph("No Data Available", self.section_style))
        elements.append(Paragraph("No jobs were found for the specified time period. This could mean:", self.styles['Normal']))
        elements.append(Paragraph("• No jobs have been processed yet", self.styles['Normal']))
        elements.append(Paragraph("• The time period may not contain any completed jobs", self.styles['Normal']))
        elements.append(Paragraph("• Jobs may still be in progress", self.styles['Normal']))
        elements.append(Spacer(1, 20))
        
        # Basic info table
        current_time = datetime.now().strftime("%B %d, %Y at %I:%M %p")
        basic_data = [
            ['Metric', 'Value'],
            ['Time Period', analytics_data.get('time_period', 'Unknown')],
            ['Total Jobs', '0'],
            ['Report Generated', current_time]
        ]
        
        basic_table = self._create_styled_table(basic_data, col_widths=[3*inch, 2*inch])
        elements.append(basic_table)
        
        return elements
    
    def _create_charts_section(self, chart_files: List[str]) -> List:
        """Create the charts section"""
        elements = []
        elements.append(Paragraph("Visual Analytics", self.subtitle_style))
        
        chart_titles = [
            "Job Status Distribution",
            "Processing Time Breakdown", 
            "Model Usage Distribution",
            "Processing Trends",
            "Quality Metrics Dashboard"
        ]
        
        for i, chart_file in enumerate(chart_files):
            if chart_file and i < len(chart_titles):
                try:
                    # Add chart title
                    elements.append(Paragraph(chart_titles[i], self.section_style))
                    
                    # Add chart image
                    img = Image(chart_file, width=6*inch, height=3.6*inch)
                    elements.append(img)
                    elements.append(Spacer(1, 20))
                    
                    # Add page break every 2 charts
                    if (i + 1) % 2 == 0 and i < len(chart_files) - 1:
                        elements.append(PageBreak())
                        
                except Exception as e:
                    print(f"Error adding chart {chart_file}: {e}")
                    continue
        
        return elements
    
    def _create_detailed_metrics(self, analytics_data: Dict[str, Any]) -> List:
        """Create detailed metrics section"""
        elements = []
        elements.append(PageBreak())
        elements.append(Paragraph("Detailed Metrics", self.subtitle_style))
        
        # Model Analytics
        model_analytics = analytics_data.get('model_analytics', {})
        if model_analytics and not model_analytics.get('error'):
            elements.append(Paragraph("Model Usage Statistics", self.section_style))
            
            model_usage = model_analytics.get('model_usage_distribution', {})
            if model_usage:
                model_data = [['Model', 'Usage Count', 'Percentage']]
                total_usage = sum(model_usage.values())
                
                for model, count in sorted(model_usage.items(), key=lambda x: x[1], reverse=True)[:10]:
                    percentage = (count / total_usage * 100) if total_usage > 0 else 0
                    model_data.append([
                        model[:40] + '...' if len(model) > 40 else model,
                        str(count),
                        f"{percentage:.1f}%"
                    ])
                
                model_table = self._create_styled_table(model_data, col_widths=[3*inch, 1*inch, 1*inch])
                elements.append(model_table)
                elements.append(Spacer(1, 20))
        
        return elements
    
    def _create_recommendations(self, analytics_data: Dict[str, Any]) -> List:
        """Create recommendations section"""
        elements = []
        elements.append(Paragraph("Recommendations", self.subtitle_style))
        
        recommendations = analytics_data.get('recommendations', [])
        if not recommendations:
            # Generate default recommendations based on data
            recommendations = self._generate_default_recommendations(analytics_data)
        
        for i, rec in enumerate(recommendations[:5]):
            title = rec.get('title', f'Recommendation {i+1}')
            description = rec.get('description', 'No description available')
            
            elements.append(Paragraph(f"{i+1}. {title}", self.section_style))
            elements.append(Paragraph(description, self.styles['Normal']))
            elements.append(Spacer(1, 15))
        
        return elements
    
    def _generate_default_recommendations(self, analytics_data: Dict[str, Any]) -> List[Dict[str, str]]:
        """Generate default recommendations based on analytics data"""
        recommendations = []
        
        # Check success rate
        perf_metrics = analytics_data.get('performance_metrics', {})
        if perf_metrics and not perf_metrics.get('error'):
            success_rate = perf_metrics.get('success_rate', 100)
            if success_rate < 90:
                recommendations.append({
                    'title': 'Improve Job Success Rate',
                    'description': f'Current success rate is {success_rate:.1f}%. Consider reviewing failed jobs and implementing better error handling.'
                })
        
        # Check processing time
        if perf_metrics:
            avg_time = perf_metrics.get('avg_processing_time_ms', 0) / 1000
            if avg_time > 5.0:
                recommendations.append({
                    'title': 'Optimize Processing Time',
                    'description': f'Average processing time is {avg_time:.2f}s. Consider optimizing AI model selection or implementing caching.'
                })
        
        # Check data volume
        total_jobs = analytics_data.get('total_jobs', 0)
        if total_jobs == 0:
            recommendations.append({
                'title': 'Increase System Usage',
                'description': 'No jobs processed in this period. Consider user training or system promotion to increase adoption.'
            })
        elif total_jobs < 10:
            recommendations.append({
                'title': 'Monitor System Adoption',
                'description': 'Low job volume detected. Monitor user adoption and provide additional training if needed.'
            })
        
        return recommendations
    
    def _create_styled_table(self, data: List[List[str]], col_widths: List = None) -> Table:
        """Create a styled table"""
        if col_widths is None:
            col_widths = [2.5*inch, 2.5*inch]
        
        table = Table(data, colWidths=col_widths)
        table.setStyle(TableStyle([
            # Header row styling
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#34495E')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 11),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('TOPPADDING', (0, 0), (-1, 0), 8),
            
            # Data rows styling
            ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#ECF0F1')),
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 1), (-1, -1), 10),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.HexColor('#ECF0F1'), colors.white]),
            
            # Grid and padding
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#BDC3C7')),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('LEFTPADDING', (0, 0), (-1, -1), 8),
            ('RIGHTPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 1), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 1), (-1, -1), 6),
        ]))
        
        return table
