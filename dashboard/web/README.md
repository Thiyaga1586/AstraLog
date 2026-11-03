# AstraLog Control Center frontend

React, TypeScript, Vite, Tailwind CSS, React Flow and Recharts. This frontend is part of the AstraLog project and is designed to visualize a local read-only metadata adapter.

## Windows CMD quick start

```bat
cd /d D:\Workspace\AstraLog\dashboard\web
npm ci
npm run dev
```

Visit `http://127.0.0.1:5173`. The frontend proxies `/api` to `127.0.0.1:8787`. Without the adapter, select **Preview** to explore a labeled illustrative dataset. To connect to real brokers, follow [`docs/CONTROL_CENTER.md`](../../docs/CONTROL_CENTER.md) from the repository root.

## Build

```bat
npm run build
```

The build includes TypeScript checking. `dist` and `node_modules` are ignored. Historical benchmark figures are fixed source-linked values from a committed release run, never described as live metrics.

The UI does not expose write, admin, broker shutdown or fault-injection controls. The local Rust adapter separately checks loopback binding and allowed CORS origins. This prototype is not intended to be deployed to an untrusted network.
