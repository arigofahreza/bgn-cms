from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import users, locations, report_user, report, maps, user_dashboard, modals
from models import locations as md_loc, users as md_us
import database

md_us.Base.metadata.create_all(bind=database.engine)
md_loc.Base.metadata.create_all(bind=database.engine)

app = FastAPI(title="BGN CMS API")

# Allow all CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # allow all
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers
app.include_router(users.router)
app.include_router(report_user.router)
app.include_router(locations.router)
app.include_router(report.router)
app.include_router(maps.router)
app.include_router(user_dashboard.router)
app.include_router(modals.router)
