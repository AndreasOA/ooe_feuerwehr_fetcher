# OÖ Feuerwehr Einsatz Tracker

![Header Banner](banner.png)

---

**OÖ Feuerwehr Einsatz Tracker** is an intuitive live emergency tracker for Upper Austria (Oberösterreich, OÖ). It provides real-time insights into ongoing emergency situations, ensuring efficient communication and coordination for those on the ground.

[**Visit the Live Tracker**](https://ooe-feuerwehr.2code.at)

## 🛠️ Code Overview

The application integrates MongoDB for data persistence and uses Streamlit for web application rendering. The Python backend handles data fetching, processing, and presentation.

- **Database Operations**: The `DbMethods` class provides methods for CRUD operations with MongoDB.
- **Utility Functions**: Helper functions such as `apply_district_abr_full`, `format_city`, and `makeMapsLink` help in data transformation and representation.

For a deep dive, refer to the provided code and the associated module imports.

---

## 📜 License & Credits

This project is open-source and available under the [MIT License](https://github.com/AndreasOA/ooe-feuerwehr-einsatz-tracker/LICENSE).

© 2024 AndreasOA

---