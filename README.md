# Amazon Last Mile Analytics

مشروع لتحويل بيانات التوصيل عبر طبقات Bronze وSilver وGold باستخدام PySpark، مع تقرير Power BI للعرض والتحليل.

## الملفات

- `normalize_*.py`: تحويل ملفات JSON الأولية إلى JSONL في `data/bronze/`.
- `bronze_to_silver_*.py`: إنشاء جداول Parquet في `output/silver/`.
- `build_gold_*.py`: إنشاء مؤشرات المسارات والمحطات في `output/gold/`.
- `data_quality_checks.py` و`gold_quality_checks.py`: فحوصات جودة البيانات.
- `AMAZON DASHBOARD.pbix`: تقرير Power BI.
- `BUILD_GUIDE.md`: دليل تخطيط صفحات التقرير ونموذج البيانات.

## التشغيل المحلي

يتطلب Python وJava وPySpark وPower BI Desktop لفتح التقرير. جميع الأوامر التالية تُنفّذ من جذر المشروع. ثبّت PySpark في بيئة Python مناسبة، ثم ضع ملفات المصدر الخاصة بك في:

| الملف | يستخدمه |
| --- | --- |
| `data/raw/route_data.json` | `normalize_routes.py` |
| `data/raw/package_data.json` | `normalize_packages.py` |
| `data/raw/actual_sequences.json` | `normalize_sequences.py` |

```bash
python -m pip install pyspark
python normalize_routes.py
python normalize_packages.py
python normalize_sequences.py
python bronze_to_silver_routes.py
python bronze_to_silver_packages.py
python bronze_to_silver_sequences.py
python data_quality_checks.py
python build_gold_route_performance.py
python build_gold_station_performance.py
python gold_quality_checks.py
```

لا تتضمن هذه النسخة ملفات البيانات الأولية، ولذلك لا يمكن تشغيل خط البيانات كاملًا دون توفيرها. المجلدان `data/` و`output/` مستبعدان من Git. قد يحتوي ملف Power BI على بيانات مخزنة داخله؛ افحصه قبل إتاحته في مستودع عام.

يشير `BUILD_GUIDE.md` إلى صور خلفية وملف theme منفصلين غير موجودين ضمن الملفات المقدمة. يحتوي ملف `.pbix` على موارد تصميم مدمجة، لكن توفر الملفات الخارجية المذكورة في الدليل غير مؤكد.
