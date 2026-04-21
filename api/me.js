export async function POST(request) {
  try {
    const { accessToken } = await request.json();

    if (!process.env.PI_API_KEY) {
      return Response.json(
        { ok: false, error: "Thiếu PI_API_KEY trên Vercel" },
        { status: 500 }
      );
    }

    if (!accessToken) {
      return Response.json(
        { ok: false, error: "Thiếu accessToken" },
        { status: 400 }
      );
    }

    const res = await fetch("https://api.minepi.com/v2/me", {
      method: "GET",
      headers: {
        "Authorization": `Bearer ${accessToken}`,
        "X-API-Key": process.env.PI_API_KEY,
        "Content-Type": "application/json"
      }
    });

    const raw = await res.text();
    let data = {};
    try {
      data = raw ? JSON.parse(raw) : {};
    } catch {
      data = { raw };
    }

    if (!res.ok) {
      return Response.json(
        {
          ok: false,
          error: data?.error || "Không verify được access token với /me",
          data
        },
        { status: res.status }
      );
    }

    const user = data?.user || data || {};
    const uid = String(user.uid || "").trim();
    const username = String(user.username || "").trim();

    if (!uid) {
      return Response.json(
        { ok: false, error: "Pi /me không trả uid hợp lệ", data },
        { status: 502 }
      );
    }

    return Response.json({
      ok: true,
      user: {
        uid,
        username
      }
    });
  } catch (err) {
    console.error("PI /me ERROR:", err);
    return Response.json(
      { ok: false, error: err?.message || "pi me error" },
      { status: 500 }
    );
  }
}