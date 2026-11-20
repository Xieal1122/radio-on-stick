"use strict";

(() => {
  const form = document.getElementById("joinForm");
  const codeInput = document.getElementById("code");
  const submit = document.getElementById("submit");
  const status = document.getElementById("status");

  const nickname = new URLSearchParams(location.search).get("nickname") || "";

  function normalize(value) {
    return String(value || "")
      .toUpperCase()
      .replace(/[IL]/g, "1")
      .replace(/O/g, "0")
      .replace(/U/g, "V")
      .replace(/[^0-9A-Z]/g, "");
  }

  function setStatus(text, tone = "") {
    status.textContent = text || "";
    status.dataset.tone = tone;
  }

  async function join(raw) {
    const code = normalize(raw);
    if (code.length < 16) {
      setStatus("请输入至少 16 位邀请码", "error");
      return;
    }
    submit.disabled = true;
    setStatus("正在验证…");
    try {
      const response = await fetch("/api/join", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ code }),
        cache: "no-store",
      });
      const data = await response.json().catch(() => ({}));
      if (!response.ok) {
        setStatus(data.message || "邀请码无效", "error");
        return;
      }
      location.replace("/");
    } catch (_) {
      setStatus("网络错误，请稍后重试", "error");
    } finally {
      submit.disabled = false;
    }
  }

  form.addEventListener("submit", (event) => {
    event.preventDefault();
    join(codeInput.value);
  });

  if (nickname) {
    const value = nickname.trim().slice(0, 16);
    try {
      localStorage.setItem("radio.nickname", value);
      const host = location.hostname;
      const domain = host.endsWith(".sslip.io") ? "; Domain=.sslip.io" : "";
      document.cookie = `radio_nickname=${encodeURIComponent(value)}; max-age=31536000; path=/; SameSite=Lax${domain}`;
    } catch (_) {
      /* ignore */
    }
  }

  const initial = normalize(decodeURIComponent(location.hash.slice(1)));
  if (initial) {
    codeInput.value = initial;
    join(initial);
  }
})();
