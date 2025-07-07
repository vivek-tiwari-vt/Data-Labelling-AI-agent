"""
Clean Analytics API Router - Uses the visual creator system
"""
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response
from datetime import datetime
import sys
import os

# Add the parent directory to the path to import common modules
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

# Import the modular analytics system
from core.analytics.analytics_core import AnalyticsCore
from core.analytics.enhanced_report_generator import EnhancedReportGenerator

router = APIRouter(prefix="/analytics", tags=["analytics"])

# Initialize components
analytics_core = AnalyticsCore()
enhanced_report_generator = EnhancedReportGenerator()

@router.get("/comprehensive")
async def get_comprehensive_analytics(
    time_period: str = Query(default="7d", description="Time period: 24h, 7d, 30d")
):
    """Get comprehensive analytics for the specified time period"""
    try:
        return analytics_core.generate_comprehensive_analytics(time_period)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get analytics: {str(e)}")

@router.get("/dashboard")
async def get_analytics_dashboard(
    time_period: str = Query(default="7d", description="Time period: 24h, 7d, 30d")
):
    """Get analytics dashboard data with charts and key metrics"""
    try:
        analytics = analytics_core.generate_comprehensive_analytics(time_period)
        
        # Extract dashboard-specific data
        return {
            "overview": {
                "total_jobs": analytics.get("total_jobs", 0),
                "success_rate": analytics.get("performance_metrics", {}).get("success_rate", 0),
                "avg_processing_time": analytics.get("performance_metrics", {}).get("avg_processing_time_ms", 0),
                "total_cost": analytics.get("cost_analysis", {}).get("total_estimated_cost", 0)
            },
            "charts": analytics.get("charts_data", {}),
            "insights": analytics.get("efficiency_insights", []),
            "recommendations": analytics.get("recommendations", []),
            "trends": analytics.get("trend_analysis", {}),
            "model_performance": analytics.get("model_analytics", {})
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get dashboard data: {str(e)}")

@router.get("/export-report")
async def export_analytics_report(
    format_type: str = Query(default="json", description="Export format: json, csv, pdf"),
    time_period: str = Query(default="7d", description="Time period: 24h, 7d, 30d")
):
    """Export comprehensive analytics report"""
    try:
        analytics = analytics_core.generate_comprehensive_analytics(time_period)
        
        if format_type.lower() == "json":
            return analytics
        elif format_type.lower() == "csv":
            # Generate CSV report
            csv_data = analytics_core.report_generator.export_data(analytics, "csv")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"analytics_report_{timestamp}.csv"
            
            return Response(
                content=csv_data,
                media_type="text/csv",
                headers={
                    "Content-Disposition": f"attachment; filename={filename}",
                    "Content-Length": str(len(csv_data))
                }
            )
        elif format_type.lower() == "pdf":
            # Generate PDF report using the enhanced report generator
            try:
                pdf_data = enhanced_report_generator.export_data(analytics, "pdf")
                
                # Verify PDF data is valid
                if not pdf_data or not pdf_data.startswith(b'%PDF'):
                    raise ValueError("Invalid PDF generated")
                
                # Generate filename with timestamp
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"analytics_report_{timestamp}.pdf"
                
                return Response(
                    content=pdf_data,
                    media_type="application/pdf",
                    headers={
                        "Content-Disposition": f"attachment; filename={filename}",
                        "Content-Length": str(len(pdf_data)),
                        "Cache-Control": "no-cache"
                    }
                )
            except Exception as e:
                # If PDF generation fails, fall back to JSON with error info
                raise HTTPException(status_code=500, detail=f"PDF generation failed: {str(e)}. Data may be insufficient for PDF report.")
        else:
            raise HTTPException(status_code=400, detail="Unsupported export format")
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to export report: {str(e)}")
