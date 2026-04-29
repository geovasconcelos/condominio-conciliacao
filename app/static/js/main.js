document.addEventListener("DOMContentLoaded", () => {
    const form = document.querySelector(".form-upload");
    if (!form) return;

    form.addEventListener("submit", () => {
        const btn = form.querySelector(".btn-analisar");
        btn.textContent = "Analisando...";
        btn.disabled = true;
    });
});
