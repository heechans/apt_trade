<!-- Daum 주소검색 API -->
<script src="https://t1.daumcdn.net/mapjsapi/bundle/postcode/prod/postcode.v2.js"></script>

<button type="button" onclick="openDaumAddress()" style="padding:8px 12px;">
    주소 검색하기
</button>

<script>
function openDaumAddress() {
    new daum.Postcode({
        oncomplete: function(data) {

            // ▼▼▼ 여기를 반드시 본인 Jotform 주소 필드 ID로 변경 ▼▼▼
            document.querySelector("#input_20_postal").value = data.zonecode;     // 우편번호
            document.querySelector("#input_20_addr_line1").value = data.address;  // 도로명 주소 or 지번 주소
            document.querySelector("#input_20_addr_line2").focus();               // 상세주소 커서 이동
            // ▲▲▲ 바꿔야 하는 부분 끝 ▲▲▲

        },
        autoClose: true
    }).open();
}
</script>

