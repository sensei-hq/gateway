// Canonical site origin (production). Used for canonical links + og:url.
export const SITE_URL = 'https://gateway.sensei-hq.com';

/** Default social share image (absolute URL). */
export const OG_IMAGE = `${SITE_URL}/favicon.svg`;

/** Absolute canonical URL for a pathname — trailing slash normalized away (except root). */
export function canonicalFor(pathname: string): string {
	const p = pathname !== '/' ? pathname.replace(/\/+$/, '') : '/';
	return SITE_URL + p;
}
