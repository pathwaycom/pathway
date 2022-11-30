### Running the app

To run the app in the dev setup you need to run two instances (e.g. in separate terminal sessions)
(make sure you have up to date environment)

1. Backend:
Go to backend/ and run `python main.py`.
The backend will be running on port 7711 (you can modify it in `main.py`, you will also have to modify BACKEND_PORT in `frontend/src/App.tsx`).

2. Frontend
go to frontend/ and run:
* `npm ci` to install packages
* `export DEV_SERVER_PORT=7712` to set port as a environment variable within your session
* `npm run dev` - this will start server hosting the frontend (it will start on port 8080 if you skipped previous step)

The app will be available under `http://172.29.3.201:DEV_SERVER_PORT/` (this assumes you run it at pathwayv1)

### Running on another machine

If you run this on a different machine (ip), you also have to update origins in backend (i.e. add entries in `origins` list in `backend/app/api.py`) and modify the BACKEND_HOST in `frontend/src/App.tsx`