import Tap from 'tap'
import App from '../src'

Tap.test('App', async (t) => {
	t.equal(App(), true)
})