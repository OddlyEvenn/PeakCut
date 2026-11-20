module.exports = {
  apps: [
    {
      name: "peakcut",
      script: "pythonw",
      args: "-m studio.server",
      cwd: "C:/Users/EVEN/Desktop/Even/PeakCut",
      interpreter: "none",
      autorestart: true,
      max_restarts: 10,
      restart_delay: 4000,
      max_memory_restart: "2500M",
      env: {
        PYTHONUNBUFFERED: "1",
        PEAKCUT_PORT: "8765",
        PEAKCUT_LOG_LEVEL: "info"
      },
      error_file: "./logs/pm2-error.log",
      out_file: "./logs/pm2-out.log",
      merge_logs: true,
      time: true
    }
  ]
};
